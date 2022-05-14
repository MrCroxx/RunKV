use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use runkv_proto::meta::KeyRange;

use super::{in_range, is_overlap, MetaStore};
use crate::error::{MetaError, Result};

type RaftStates = Arc<RwLock<HashMap<u64, Option<raft::SoftState>>>>;

#[derive(Default)]
struct MemoryMetaStoreCore {
    /// `{ [start key .. end key) -> (group, [raft node 1, range node 2, ..]) }`
    key_ranges: BTreeMap<KeyRange, (u64, Vec<u64>)>,
}

#[derive(Default)]
pub struct MemoryMetaStore {
    inner: RwLock<MemoryMetaStoreCore>,

    raft_states: RaftStates,
}

impl MemoryMetaStore {}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn add_key_range(
        &self,
        key_range: KeyRange,
        group: u64,
        raft_nodes: &[u64],
    ) -> Result<()> {
        let mut guard = self.inner.write();
        for r in guard.key_ranges.keys() {
            if is_overlap(r, &key_range) {
                return Err(MetaError::KeyRangeOverlaps {
                    r1: r.to_owned(),
                    r2: key_range,
                }
                .into());
            }
        }
        guard
            .key_ranges
            .insert(key_range, (group, raft_nodes.to_vec()));
        Ok(())
    }

    async fn key_ranges(&self) -> Result<Vec<KeyRange>> {
        let guard = self.inner.read();
        Ok(guard.key_ranges.keys().cloned().collect_vec())
    }

    async fn in_range(&self, key: &[u8]) -> Result<Option<(KeyRange, u64, Vec<u64>)>> {
        let guard = self.inner.read();
        for (r, (group, raft_nodes)) in guard.key_ranges.iter() {
            if in_range(key, r) {
                return Ok(Some((r.to_owned(), *group, raft_nodes.to_owned())));
            }
        }
        Ok(None)
    }

    async fn all_in_range(&self, keys: &[&[u8]]) -> Result<Option<(KeyRange, u64, Vec<u64>)>> {
        if keys.is_empty() {
            return Ok(None);
        }
        let guard = self.inner.read();
        let mut result = None;
        for (r, (group, raft_nodes)) in guard.key_ranges.iter() {
            if in_range(keys[0], r) {
                result = Some((r.to_owned(), *group, raft_nodes.to_owned()));
                break;
            }
        }
        if result.is_none() {
            return Ok(None);
        }
        let (range, group, raft_nodes) = result.unwrap();
        for key in &keys[1..] {
            if !in_range(key, &range) {
                return Ok(None);
            }
        }
        Ok(Some((range, group, raft_nodes)))
    }

    async fn update_raft_state(
        &self,
        raft_node: u64,
        raft_state: Option<raft::SoftState>,
    ) -> Result<()> {
        let mut raft_states = self.raft_states.write();
        raft_states.insert(raft_node, raft_state);
        Ok(())
    }

    async fn all_raft_states(&self) -> Result<HashMap<u64, Option<raft::SoftState>>> {
        Ok(self.raft_states.read().clone())
    }

    async fn is_raft_leader(&self, raft_node: u64) -> Result<bool> {
        let raft_states = self.raft_states.read();
        let is_leader = match raft_states.get(&raft_node) {
            None | Some(None) => false,
            Some(Some(ss)) => ss.raft_state == raft::StateRole::Leader,
        };
        Ok(is_leader)
    }
}
