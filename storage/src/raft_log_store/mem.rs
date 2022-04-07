use std::collections::btree_map::{BTreeMap, Entry};

use tokio::sync::RwLock;

use super::error::RaftLogStoreError;
use crate::error::Result;

const DEFAULT_INDICES_INIT_CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub struct EntryIndex {
    /// Prevent log entries with lower terms added from GC shadowing those with the same indices
    /// but with higher terms.
    pub term: u64,
    pub block_offset: usize,
    pub block_len: usize,
    pub offset: usize,
    pub len: usize,
}

pub struct MemState {
    first_index: u64,
    indices: Vec<EntryIndex>,
    kvs: BTreeMap<Vec<u8>, Vec<u8>>,
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
    pub async fn add_group(&self, group: u64, first_index: u64) -> Result<()> {
        let mut guard = self.states.write().await;
        match guard.entry(group) {
            Entry::Occupied(_) => return Err(RaftLogStoreError::GroupAlreadyExists(group).into()),
            Entry::Vacant(v) => {
                v.insert(RwLock::new(MemState {
                    first_index,
                    indices: Vec::with_capacity(DEFAULT_INDICES_INIT_CAPACITY),
                    kvs: BTreeMap::default(),
                }));
            }
        }
        Ok(())
    }

    /// # Safety
    ///
    /// Removed group needs to be guaranteed never be used again.
    pub async fn remove_group(&self, group: u64) -> Result<()> {
        let mut guard = self.states.write().await;
        match guard.entry(group) {
            Entry::Occupied(o) => {
                let mut state = o.into_mut().write().await;
                state.first_index = u64::MAX;
                state.indices.clear();
                state.kvs.clear();
            }
            Entry::Vacant(_) => return Err(RaftLogStoreError::GroupNotExists(group).into()),
        }
        Ok(())
    }

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

    /// Append raft log indices.
    pub async fn append(
        &self,
        group: u64,
        mut first_index: u64,
        mut indices: Vec<EntryIndex>,
    ) -> Result<()> {
        debug_assert!(!indices.is_empty());
        let guard = self.states.read().await;
        let mut state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .write()
            .await;

        let state_next_index = state.first_index + state.indices.len() as u64;

        // Return error if there is gap in raft log.
        if first_index > state_next_index {
            return Err(RaftLogStoreError::RaftLogGap {
                start: state_next_index,
                end: first_index,
            }
            .into());
        }

        // Ignore outdated indices.
        if first_index < state.first_index {
            indices.drain(..(state.first_index - first_index) as usize);
            first_index = state.first_index;
            if indices.is_empty() {
                return Ok(());
            }
        }

        // Directly append new indices.
        if (state_next_index as usize - first_index as usize) < indices.len() {
            let next_indices = indices.drain((state_next_index - first_index) as usize..);
            state.indices.extend(next_indices)
        }

        // Update overlapping indices.
        let overlap_start = (first_index - state.first_index) as usize;
        let overlap_end = overlap_start + indices.len();
        for (indices_i, state_indices_i) in (overlap_start..overlap_end).enumerate() {
            let state_index = &mut state.indices[state_indices_i];
            let index = &mut indices[indices_i];

            // Ignore outdated rewrite indices.
            if state_index.term > index.term {
                continue;
            }

            *state_index = *index;
        }

        Ok(())
    }

    /// Compact any indices before the given index.
    pub async fn compact(&self, group: u64, index: u64) -> Result<()> {
        let guard = self.states.read().await;
        let mut state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .write()
            .await;

        // Ignore outdated compact command.
        if index <= state.first_index {
            return Ok(());
        }

        // Return error if there is gap in raft log.
        if index > state.first_index + state.indices.len() as u64 {
            return Err(RaftLogStoreError::RaftLogGap {
                start: state.first_index + state.indices.len() as u64,
                end: index,
            }
            .into());
        }

        // Truncate indices.
        let len = (index - state.first_index) as usize;
        state.indices.drain(..len);
        state.first_index = index;

        Ok(())
    }

    pub async fn put(&self, group: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let guard = self.states.read().await;
        let mut state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .write()
            .await;
        state.kvs.insert(key, value);
        Ok(())
    }

    pub async fn delete(&self, group: u64, key: Vec<u8>) -> Result<()> {
        let guard = self.states.read().await;
        let mut state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .write()
            .await;
        state.kvs.remove(&key);
        Ok(())
    }

    pub async fn get(&self, group: u64, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let guard = self.states.read().await;
        let state = guard
            .get(&group)
            .ok_or(RaftLogStoreError::GroupNotExists(group))?
            .read()
            .await;
        Ok(state.kvs.get(&key).cloned())
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Range;

    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    async fn test_raft_log() {
        let states = MemStates::default();
        states.add_group(1, 1).await.unwrap();
        states.append(1, 1, gen_indices(1, 100)).await.unwrap();
        assert_range(&states, 1, 1..101).await;
        assert!(states.append(1, 102, gen_indices(1, 100)).await.is_err());
        assert_range(&states, 1, 1..101).await;
        states.append(1, 101, gen_indices(1, 100)).await.unwrap();
        assert_range(&states, 1, 1..201).await;
        states.compact(1, 101).await.unwrap();
        assert_range(&states, 1, 101..201).await;
        states.append(1, 51, gen_indices(1, 200)).await.unwrap();
        assert_range(&states, 1, 101..251).await;
        states.compact(1, 251).await.unwrap();
        assert_range(&states, 1, 251..251).await;
        assert!(states.compact(1, 252).await.is_err());
        states.compact(1, 101).await.unwrap();
        states.remove_group(1).await.unwrap();
    }

    #[test(tokio::test)]
    async fn test_kv() {
        let states = MemStates::default();
        states.add_group(1, 1).await.unwrap();
        states.put(1, b"k1".to_vec(), b"v1".to_vec()).await.unwrap();
        assert_eq!(
            states.get(1, b"k1".to_vec()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        states.put(1, b"k1".to_vec(), b"v2".to_vec()).await.unwrap();
        assert_eq!(
            states.get(1, b"k1".to_vec()).await.unwrap(),
            Some(b"v2".to_vec())
        );
        states.delete(1, b"k1".to_vec()).await.unwrap();
        assert_eq!(states.get(1, b"k1".to_vec()).await.unwrap(), None);
        states.remove_group(1).await.unwrap();
    }

    async fn assert_range(target: &MemStates, group: u64, range: Range<u64>) {
        let guard = target.states.read().await;
        let state = guard.get(&group).unwrap().read().await;
        assert_eq!(
            (
                state.first_index,
                state.first_index + state.indices.len() as u64
            ),
            (range.start, range.end)
        );
    }

    fn gen_indices(term: u64, len: usize) -> Vec<EntryIndex> {
        vec![
            EntryIndex {
                term,
                block_offset: 0,
                block_len: 0,
                offset: 0,
                len: 0,
            };
            len
        ]
    }
}
