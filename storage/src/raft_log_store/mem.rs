use std::collections::BTreeMap;

use tokio::sync::RwLock;

use super::error::RaftLogStoreError;
use crate::error::Result;

pub struct EntryIndex {
    /// Prevent log entries with lower terms added from GC shadowing those with the same indices
    /// but with higher terms.
    pub term: u64,
    pub block_offset: u64,
    pub block_len: usize,
    pub offset: u64,
    pub len: usize,
}

pub struct MemState {
    group: u64,
    first_index: u64,
    indices: Vec<EntryIndex>,
    kvs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemState {
    pub fn new(group: u64, last_index: u64, init_capacity: usize) -> Self {
        Self {
            group,
            first_index: last_index,
            indices: Vec::with_capacity(init_capacity),
            kvs: BTreeMap::default(),
        }
    }
}

pub struct MemStates {
    /// Mapping [`group`] to [`MemState`].
    states: RwLock<BTreeMap<u64, RwLock<MemState>>>,
}

impl Default for MemStates {
    fn default() -> Self {
        Self {
            states: RwLock::new(BTreeMap::default()),
        }
    }
}

impl MemStates {
    pub async fn first_index(&self, group: u64) -> Result<u64> {
        let guard = self.states.read().await;
        let state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .read()
            .await;
        Ok(state.first_index)
    }

    pub async fn last_index(&self, group: u64) -> Result<u64> {
        let guard = self.states.read().await;
        let state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .read()
            .await;
        Ok(state.first_index + state.indices.len() as u64 - 1)
    }
}
