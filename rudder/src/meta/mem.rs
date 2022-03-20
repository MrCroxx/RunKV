use std::collections::BTreeMap;
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
}

pub struct MemoryMetaStore {
    inner: RwLock<MemoryMetaStoreCore>,
}

impl Default for MemoryMetaStore {
    fn default() -> Self {
        Self {
            inner: RwLock::new(MemoryMetaStoreCore::default()),
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
}
