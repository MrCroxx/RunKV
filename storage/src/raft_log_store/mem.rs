use std::collections::btree_map::{BTreeMap, Entry};

use itertools::Itertools;
use tokio::sync::RwLock;
use tracing::trace;

use super::error::RaftLogStoreError;
use crate::error::Result;

const DEFAULT_INDICES_INIT_CAPACITY: usize = 1024;

macro_rules! state {
    ($states:expr, $group:expr, $guard:ident, $state:ident) => {
        let $guard = $states.read().await;
        let $state = $guard
            .get(&$group)
            .ok_or(RaftLogStoreError::GroupNotExists($group))?
            .read()
            .await;
    };
}

macro_rules! state_mut {
    ($states:expr, $group:expr, $guard:ident, $state:ident) => {
        let $guard = $states.read().await;
        let mut $state = $guard
            .get(&$group)
            .ok_or(RaftLogStoreError::GroupNotExists($group))?
            .write()
            .await;
    };
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct EntryIndex {
    /// Prevent log entries with lower terms added from GC shadowing those with the same indices
    /// but with higher terms.
    pub term: u64,
    pub ctx: Vec<u8>,
    pub file_id: u64,
    pub block_offset: usize,
    pub block_len: usize,
    pub offset: usize,
    pub len: usize,
}

pub struct MemState {
    first_index: u64,
    mask_index: u64,
    indices: Vec<EntryIndex>,
    kvs: BTreeMap<Vec<u8>, Vec<u8>>,
    phantom_term: u64,
}

pub struct MemStates {
    node: u64,

    /// Mapping [`group`] to [`MemState`].
    states: RwLock<BTreeMap<u64, RwLock<MemState>>>,
}

impl std::fmt::Debug for MemStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStates")
            .field("node", &self.node)
            .finish()
    }
}

impl MemStates {
    pub fn new(node: u64) -> Self {
        Self {
            node,

            states: RwLock::new(BTreeMap::default()),
        }
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn add_group(&self, group: u64) -> Result<()> {
        let mut guard = self.states.write().await;
        match guard.entry(group) {
            Entry::Occupied(_) => return Err(RaftLogStoreError::GroupAlreadyExists(group).into()),
            Entry::Vacant(v) => {
                v.insert(RwLock::new(MemState {
                    first_index: 1,
                    mask_index: 1,
                    indices: Vec::with_capacity(DEFAULT_INDICES_INIT_CAPACITY),
                    kvs: BTreeMap::default(),
                    phantom_term: 0,
                }));
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", ret)]
    pub async fn may_add_group(&self, group: u64) -> bool {
        let mut guard = self.states.write().await;
        match guard.entry(group) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(RwLock::new(MemState {
                    first_index: 1,
                    mask_index: 1,
                    indices: Vec::with_capacity(DEFAULT_INDICES_INIT_CAPACITY),
                    kvs: BTreeMap::default(),
                    phantom_term: 0,
                }));
                true
            }
        }
    }

    /// # Safety
    ///
    /// Removed group needs to be guaranteed never be used again.
    #[tracing::instrument(level = "trace", ret, err)]
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

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn term(&self, group: u64, index: u64) -> Result<Option<u64>> {
        state!(self.states, group, guard, state);

        if state.indices.is_empty() || index == state.first_index - 1 {
            return Ok(Some(state.phantom_term));
        }

        if index < state.first_index || index >= state.first_index + state.indices.len() as u64 {
            Ok(None)
        } else {
            let i = (index - state.first_index) as usize;
            let term = state.indices[i].term;
            Ok(Some(term))
        }
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn ctx(&self, group: u64, index: u64) -> Result<Option<Vec<u8>>> {
        state!(self.states, group, guard, state);

        if index < state.first_index || index >= state.first_index + state.indices.len() as u64 {
            Ok(None)
        } else {
            let i = (index - state.first_index) as usize;
            let ctx = state.indices[i].ctx.clone();
            Ok(Some(ctx))
        }
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn first_index(&self, group: u64) -> Result<u64> {
        state!(self.states, group, guard, state);

        Ok(state.first_index)
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn last_index(&self, group: u64) -> Result<u64> {
        state!(self.states, group, guard, state);

        let last_index = state.first_index + state.indices.len() as u64 - 1;
        Ok(last_index)
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn masked_first_index(&self, group: u64) -> Result<u64> {
        state!(self.states, group, guard, state);

        Ok(state.mask_index)
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn masked_last_index(&self, group: u64) -> Result<u64> {
        state!(self.states, group, guard, state);

        let mask_last_index = if state.indices.is_empty() {
            state.mask_index - 1
        } else {
            state.first_index + state.indices.len() as u64 - 1
        };
        Ok(mask_last_index)
    }

    /// Append raft log indices.
    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn append(
        &self,
        group: u64,
        mut first_index: u64,
        mut indices: Vec<EntryIndex>,
    ) -> Result<()> {
        debug_assert!(!indices.is_empty());
        state_mut!(self.states, group, guard, state);

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

            *state_index = index.clone();
        }

        Ok(())
    }

    /// Truncate raft log of given `group` since given `index`.
    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn truncate(&self, group: u64, index: u64) -> Result<()> {
        state_mut!(self.states, group, guard, state);

        if index < state.first_index {
            return Err(RaftLogStoreError::RaftLogGap {
                start: index,
                end: state.first_index,
            }
            .into());
        }

        if index >= state.first_index + state.indices.len() as u64 {
            return Err(RaftLogStoreError::RaftLogGap {
                start: state.first_index + state.indices.len() as u64,
                end: index,
            }
            .into());
        }

        let len = (index - state.first_index) as usize;
        state.indices.truncate(len);

        Ok(())
    }

    /// Compact any indices before the given index.
    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn compact(&self, group: u64, index: u64) -> Result<()> {
        state_mut!(self.states, group, guard, state);

        // Ignore outdated compact command.
        if index <= state.first_index {
            return Ok(());
        }

        // If given index is greater than `next_index`, clear all indices and set `first_index` to
        // compact index.
        if index > state.first_index + state.indices.len() as u64 {
            // Accurate term cannot be known for there is log gap. Use the largest one instead.
            if let Some(index) = state.indices.last() {
                state.phantom_term = index.term;
            }
            state.first_index = index;
            state.indices.clear();
            return Ok(());
        }

        // Compact indices.
        let len = (index - state.first_index) as usize;
        if let Some(index) = state.indices.drain(..len).last() {
            state.phantom_term = index.term;
        }
        state.first_index = index;

        Ok(())
    }

    /// Mask any indices before the given index.
    ///
    /// Masked indices are not deleted from the state, they should not be accessed by raft
    /// algorithm.
    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn mask(&self, group: u64, index: u64) -> Result<()> {
        state_mut!(self.states, group, guard, state);

        if index > state.first_index + state.indices.len() as u64 {
            return Err(RaftLogStoreError::RaftLogGap {
                start: state.first_index + state.indices.len() as u64,
                end: index,
            }
            .into());
        }
        state.mask_index = index;

        Ok(())
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn may_entries(
        &self,
        group: u64,
        index: u64,
        max_len: usize,
        unmask: bool,
    ) -> Result<(u64, Vec<EntryIndex>)> {
        state!(self.states, group, guard, state);

        let start_index = std::cmp::max(
            index,
            if unmask {
                state.first_index
            } else {
                std::cmp::max(state.mask_index, state.first_index)
            },
        );
        let end_index = std::cmp::min(
            index + max_len as u64,
            state.first_index + state.indices.len() as u64,
        );

        if start_index >= end_index {
            return Ok((0, vec![]));
        }

        trace!(
            "get indices [{}..{}) out of [{}..{}), mask at {}, unmask: {}",
            start_index,
            end_index,
            state.first_index,
            state.first_index + state.indices.len() as u64,
            state.mask_index,
            unmask,
        );

        let start = (start_index - state.first_index) as usize;
        let end = start + (end_index - start_index) as usize;

        let indices = (&state.indices[start..end]).iter().cloned().collect_vec();
        Ok((start_index, indices))
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn entries(&self, group: u64, index: u64, max_len: usize) -> Result<Vec<EntryIndex>> {
        state!(self.states, group, guard, state);

        if index < state.first_index {
            return Err(RaftLogStoreError::RaftLogGap {
                start: index,
                end: state.first_index,
            }
            .into());
        }

        if index >= state.first_index + state.indices.len() as u64 {
            return Err(RaftLogStoreError::RaftLogGap {
                start: state.first_index + state.indices.len() as u64,
                end: index,
            }
            .into());
        }

        let start = (index - state.first_index) as usize;
        let end = std::cmp::min(start + max_len, state.indices.len());

        let indices = (&state.indices[start..end]).iter().cloned().collect_vec();
        Ok(indices)
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn put(&self, group: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        state_mut!(self.states, group, guard, state);

        state.kvs.insert(key, value);
        Ok(())
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn delete(&self, group: u64, key: Vec<u8>) -> Result<()> {
        state_mut!(self.states, group, guard, state);

        state.kvs.remove(&key);
        Ok(())
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn get(&self, group: u64, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        state!(self.states, group, guard, state);

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
        let states = MemStates::new(0);

        states.add_group(1).await.unwrap();

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
        // assert!(states.compact(1, 252).await.is_err());
        states.compact(1, 101).await.unwrap();

        states.append(1, 251, gen_indices(2, 100)).await.unwrap();
        assert_range(&states, 1, 251..351).await;
        assert_eq!(
            states.entries(1, 251, usize::MAX).await.unwrap(),
            gen_indices(2, 100)
        );
        states.append(1, 301, gen_indices(3, 100)).await.unwrap();
        assert_range(&states, 1, 251..401).await;
        assert_eq!(
            states.entries(1, 251, usize::MAX).await.unwrap(),
            [gen_indices(2, 50), gen_indices(3, 100)].concat(),
        );
        states.append(1, 1, gen_indices(1, 400)).await.unwrap();
        assert_range(&states, 1, 251..401).await;
        assert_eq!(
            states.entries(1, 251, usize::MAX).await.unwrap(),
            [gen_indices(2, 50), gen_indices(3, 100)].concat(),
        );
        assert!(states.entries(1, 250, usize::MAX).await.is_err());
        assert!(states.entries(1, 401, usize::MAX).await.is_err());

        assert!(states.truncate(1, 250).await.is_err());
        assert!(states.truncate(1, 401).await.is_err());
        states.truncate(1, 301).await.unwrap();
        assert_range(&states, 1, 251..301).await;

        states.remove_group(1).await.unwrap();
    }

    #[test(tokio::test)]
    async fn test_kv() {
        let states = MemStates::new(0);
        states.add_group(1).await.unwrap();
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
                ctx: vec![],
                file_id: 1,
                block_offset: 0,
                block_len: 0,
                offset: 0,
                len: 0,
            };
            len
        ]
    }
}
