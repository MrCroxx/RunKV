use std::collections::btree_map::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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
    core: RwLock<MemoryMetaStoreCore>,
    txn: AtomicU32,
    watermark: AtomicU64,
}

impl MemoryMetaStore {
    pub fn new(sstable_pin_ttl: Duration) -> Self {
        Self {
            core: RwLock::new(MemoryMetaStoreCore {
                sstable_pin_ttl,
                ..Default::default()
            }),
            txn: AtomicU32::new(1),
            watermark: AtomicU64::new(1 << 32),
        }
    }
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn update_exhauster(&self, node_id: u64, endpoint: PbEndpoint) -> Result<()> {
        let heartbeat = SystemTime::now();
        self.core.write().exhausters.insert(
            node_id,
            ExhausterInfo {
                endpoint,
                heartbeat,
            },
        );
        Ok(())
    }

    async fn pick_exhauster(&self, live: Duration) -> Result<Option<PbEndpoint>> {
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
                return Ok(Some(info.endpoint.clone()));
            }
        }
        Ok(None)
    }

    async fn update_node_ranges(&self, node_id: u64, ranges: Vec<KeyRange>) -> Result<()> {
        let mut guard = self.core.write();
        guard.node_ranges.insert(node_id, ranges);
        Ok(())
    }

    async fn all_node_ranges(&self) -> Result<BTreeMap<u64, Vec<KeyRange>>> {
        let guard = self.core.read();
        let node_ranges = guard.node_ranges.clone();
        Ok(node_ranges)
    }

    async fn all_ranges(&self) -> Result<Vec<KeyRange>> {
        let guard = self.core.read();
        let ranges = guard
            .node_ranges
            .iter()
            .flat_map(|(_node_id, ranges)| ranges.clone())
            .collect_vec();
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
        Ok(true)
    }

    async fn unpin_sstables(&self, sst_ids: &[u64]) -> Result<()> {
        let mut guard = self.core.write();
        for sst_id in sst_ids.iter() {
            guard.pinned_sstables.remove(sst_id);
        }
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

    async fn txn(&self) -> Result<u32> {
        Ok(self.txn.load(Ordering::SeqCst))
    }

    async fn txn_fetch_add(&self, val: u32) -> Result<u32> {
        Ok(self.txn.fetch_add(val, Ordering::SeqCst))
    }

    async fn watermark(&self) -> Result<u64> {
        Ok(self.watermark.load(Ordering::Acquire))
    }

    async fn update_watermark(&self, watermark: u64) -> Result<()> {
        self.watermark.store(watermark, Ordering::Release);
        Ok(())
    }
}
