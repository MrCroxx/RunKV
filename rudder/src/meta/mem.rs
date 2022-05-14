use std::collections::btree_map::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use runkv_proto::common::Endpoint;
use runkv_proto::meta::{KeyRange, KeyRangeInfo};
use runkv_proto::rudder::RaftState;
use tracing::trace;

use super::{is_overlap, MetaStore};
use crate::error::{ControlError, Result};

const RAFT_STATE_TIMEOUT_DURATION: Duration = Duration::from_secs(10);

struct ExhausterInfo {
    _endpoint: Endpoint,
    // TODO: Track pressure.
    heartbeat: SystemTime,
}

#[derive(Default)]
pub struct MemoryMetaStoreCore {
    key_range_infos: Vec<KeyRangeInfo>,

    /// { (wheel) node id -> endpoint }
    wheels: BTreeMap<u64, Endpoint>,
    /// { (exhauster) node id -> exhauster info }
    exhausters: BTreeMap<u64, ExhausterInfo>,

    /// { key range -> raft group id}
    key_range_groups: BTreeMap<KeyRange, u64>,
    /// { node id -> [raft node id] }
    node_raft_nodes: BTreeMap<u64, Vec<u64>>,
    /// { raft group id -> [raft node id] }
    group_raft_nodes: BTreeMap<u64, Vec<u64>>,
    /// { raft node id -> raft group id }
    raft_node_groups: BTreeMap<u64, u64>,

    /// { raft node id -> (raft state, update timestamp) }
    raft_states: HashMap<u64, (RaftState, SystemTime)>,

    pinned_sstables: BTreeMap<u64, SystemTime>,
    sstable_pin_ttl: Duration,
}

pub struct MemoryMetaStore {
    core: RwLock<MemoryMetaStoreCore>,
    timestamp: AtomicU32,
}

impl MemoryMetaStore {
    pub fn new(sstable_pin_ttl: Duration) -> Self {
        Self {
            core: RwLock::new(MemoryMetaStoreCore {
                sstable_pin_ttl,
                ..Default::default()
            }),
            timestamp: AtomicU32::new(1),
        }
    }
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn add_wheels(&self, endpoints: HashMap<u64, Endpoint>) -> Result<()> {
        let mut core = self.core.write();
        for (node, endpoint) in endpoints {
            if let Some(origin) = core.wheels.get(&node) {
                return Err(ControlError::NodeAlreadyExists {
                    node,
                    origin: origin.clone(),
                    given: endpoint,
                }
                .into());
            }
            core.wheels.insert(node, endpoint);
        }
        Ok(())
    }

    async fn wheels(&self) -> Result<HashMap<u64, Endpoint>> {
        let core = self.core.read();
        let wheels = HashMap::from_iter(
            core.wheels
                .iter()
                .map(|(&node, endpoint)| (node, endpoint.clone())),
        );
        Ok(wheels)
    }

    async fn add_key_ranges(&self, key_ranges: Vec<KeyRangeInfo>) -> Result<()> {
        let mut core = self.core.write();
        for info in key_ranges {
            let key_range = info.key_range.clone().unwrap();
            let group = info.group;
            let raft_nodes = info.raft_nodes.clone();

            if core.group_raft_nodes.get(&group).is_some() {
                return Err(ControlError::GroupAlreadyExists(group).into());
            }
            for r in core.key_range_groups.keys() {
                if is_overlap(&key_range, r) {
                    return Err(ControlError::KeyRangeOverlaps(key_range, r.clone()).into());
                }
            }
            for (raft_node, node) in raft_nodes.iter() {
                if core.raft_node_groups.get(raft_node).is_some() {
                    return Err(ControlError::RaftNodeAlreadyExists(*raft_node).into());
                }
                if core.wheels.get(node).is_none() {
                    return Err(ControlError::NodeNotExists(*node).into());
                }
            }

            core.key_range_groups.insert(key_range, group);
            core.group_raft_nodes
                .insert(group, raft_nodes.keys().copied().collect_vec());
            for (&raft_node, &node) in raft_nodes.iter() {
                core.node_raft_nodes
                    .entry(node)
                    .or_insert_with(|| Vec::with_capacity(16))
                    .push(raft_node);
                core.raft_node_groups.insert(raft_node, group);
            }

            core.key_range_infos.push(info);
        }
        Ok(())
    }

    async fn all_key_range_infos(&self) -> Result<Vec<KeyRangeInfo>> {
        let core = self.core.read();
        let mut key_range_infos = core.key_range_infos.clone();
        let now = SystemTime::now();
        let mut leaders = HashMap::new();
        for (&raft_node, (raft_state, ts)) in core.raft_states.iter() {
            if now.duration_since(*ts).unwrap() > RAFT_STATE_TIMEOUT_DURATION {
                continue;
            }
            if raft_state.is_leader {
                let group = *core.raft_node_groups.get(&raft_node).unwrap();
                leaders.insert(group, raft_node);
            }
        }
        for info in key_range_infos.iter_mut() {
            info.leader = leaders.get(&info.group).copied().unwrap_or(0);
        }
        Ok(key_range_infos)
    }

    async fn update_raft_states(&self, raft_states: HashMap<u64, RaftState>) -> Result<()> {
        let mut core = self.core.write();
        let now = SystemTime::now();
        for (raft_node, raft_state) in raft_states {
            core.raft_states.insert(raft_node, (raft_state, now));
        }
        Ok(())
    }

    async fn update_exhauster(&self, node_id: u64, endpoint: Endpoint) -> Result<()> {
        let heartbeat = SystemTime::now();
        self.core.write().exhausters.insert(
            node_id,
            ExhausterInfo {
                _endpoint: endpoint,
                heartbeat,
            },
        );
        Ok(())
    }

    async fn pick_exhauster(&self, live: Duration) -> Result<Option<u64>> {
        let guard = self.core.read();
        let mut exhauster_ids = guard.exhausters.keys().collect_vec();
        exhauster_ids.shuffle(&mut thread_rng());
        for exhauster_id in exhauster_ids {
            let info = guard.exhausters.get(exhauster_id).unwrap();
            let duration = info
                .heartbeat
                .elapsed()
                .expect("last heartbeat time must be earilier than now");
            if duration <= live {
                return Ok(Some(*exhauster_id));
            }
        }
        Ok(None)
    }

    async fn all_group_key_ranges(&self) -> Result<BTreeMap<u64, Vec<KeyRange>>> {
        let core = self.core.read();
        let mut group_key_ranges = BTreeMap::default();
        for (key_range, &group) in core.key_range_groups.iter() {
            group_key_ranges
                .entry(group)
                .or_insert_with(Vec::new)
                .push(key_range.clone());
        }
        Ok(group_key_ranges)
    }

    async fn all_key_ranges(&self) -> Result<Vec<KeyRange>> {
        let core = self.core.read();
        let ranges = core.key_range_groups.keys().cloned().collect_vec();
        Ok(ranges)
    }

    async fn pin_sstables(&self, sst_ids: &[u64], time: SystemTime) -> Result<bool> {
        let mut guard = self.core.write();
        let mut pin = true;
        for sst_id in sst_ids.iter() {
            if guard.pinned_sstables.get(sst_id).map_or_else(
                || false,
                |last_pin_time| *last_pin_time + guard.sstable_pin_ttl < time,
            ) {
                pin = false;
                break;
            }
        }
        if !pin {
            return Ok(false);
        }
        for sst_id in sst_ids.iter() {
            guard.pinned_sstables.insert(*sst_id, time);
        }
        trace!(
            "pin - pinning ssts: {:?}",
            guard.pinned_sstables.keys().collect_vec(),
        );
        Ok(true)
    }

    async fn unpin_sstables(&self, sst_ids: &[u64]) -> Result<()> {
        let mut guard = self.core.write();
        for sst_id in sst_ids.iter() {
            guard.pinned_sstables.remove(sst_id);
        }
        trace!(
            "unpin - pinning ssts: {:?}",
            guard.pinned_sstables.keys().collect_vec(),
        );
        Ok(())
    }

    async fn is_sstables_pinned(&self, sst_ids: &[u64], time: SystemTime) -> Result<Vec<bool>> {
        let mut pinned = Vec::with_capacity(sst_ids.len());
        let guard = self.core.read();
        for sst_id in sst_ids {
            pinned.push(guard.pinned_sstables.get(sst_id).map_or_else(
                || false,
                |last_pin_time| *last_pin_time + guard.sstable_pin_ttl >= time,
            ));
        }
        Ok(pinned)
    }

    /// Get the current timestamp.
    async fn timestamp(&self) -> Result<u32> {
        Ok(self.timestamp.load(Ordering::SeqCst))
    }

    /// Fetch the current timestamp and advance it by `val`.
    async fn timestamp_fetch_add(&self, val: u32) -> Result<u32> {
        Ok(self.timestamp.fetch_add(val, Ordering::SeqCst))
    }
}
