use std::collections::btree_map::BTreeMap;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::meta::KeyRange;

use super::MetaStore;
use crate::error::Result;

struct ExhausterInfo {
    endpoint: PbEndpoint,
    heartbeat: SystemTime,
}

#[derive(Default)]
pub struct MemoryMetaStoreCore {
    exhausters: BTreeMap<u64, ExhausterInfo>,
    node_ranges: BTreeMap<u64, Vec<KeyRange>>,
    pinned_sstables: BTreeMap<u64, SystemTime>,
    sstable_pin_ttl: Duration,
}

pub struct MemoryMetaStore {
    inner: RwLock<MemoryMetaStoreCore>,
}

impl MemoryMetaStore {
    pub fn new(sstable_pin_ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(MemoryMetaStoreCore {
                sstable_pin_ttl,
                ..Default::default()
            }),
        }
    }
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn update_exhauster(&self, node_id: u64, endpoint: PbEndpoint) -> Result<()> {
        let heartbeat = SystemTime::now();
        self.inner.write().exhausters.insert(
            node_id,
            ExhausterInfo {
                endpoint,
                heartbeat,
            },
        );
        Ok(())
    }

    async fn pick_exhauster(&self, live: Duration) -> Result<Option<PbEndpoint>> {
        let guard = self.inner.read();
        let mut exhauster_ids = guard.exhausters.keys().collect_vec();
        exhauster_ids.shuffle(&mut thread_rng());
        for exhauster_id in exhauster_ids {
            let info = guard.exhausters.get(exhauster_id).unwrap();
            let duration = info
                .heartbeat
                .elapsed()
                .expect("last heartbeat time must be earilier than now");
            if duration <= live {
                return Ok(Some(info.endpoint.clone()));
            }
        }
        Ok(None)
    }

    async fn update_node_ranges(&self, node_id: u64, ranges: Vec<KeyRange>) -> Result<()> {
        let mut guard = self.inner.write();
        guard.node_ranges.insert(node_id, ranges);
        Ok(())
    }

    async fn all_node_ranges(&self) -> Result<BTreeMap<u64, Vec<KeyRange>>> {
        let guard = self.inner.read();
        let node_ranges = guard.node_ranges.clone();
        Ok(node_ranges)
    }

    async fn all_ranges(&self) -> Result<Vec<KeyRange>> {
        let guard = self.inner.read();
        let ranges = guard
            .node_ranges
            .iter()
            .flat_map(|(_node_id, ranges)| ranges.clone())
            .collect_vec();
        Ok(ranges)
    }

    async fn pin_sstables(&self, sst_ids: &[u64], time: SystemTime) -> Result<bool> {
        let mut guard = self.inner.write();
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
        Ok(true)
    }

    async fn unpin_sstables(&self, sst_ids: &[u64]) -> Result<()> {
        let mut guard = self.inner.write();
        for sst_id in sst_ids.iter() {
            guard.pinned_sstables.remove(sst_id);
        }
        Ok(())
    }

    async fn is_sstables_pinned(&self, sst_ids: &[u64], time: SystemTime) -> Result<Vec<bool>> {
        let mut pinned = Vec::with_capacity(sst_ids.len());
        let guard = self.inner.read();
        for sst_id in sst_ids {
            pinned.push(guard.pinned_sstables.get(sst_id).map_or_else(
                || false,
                |last_pin_time| *last_pin_time + guard.sstable_pin_ttl >= time,
            ));
        }
        Ok(pinned)
    }
}
