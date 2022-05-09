use async_trait::async_trait;
use bytes::{Buf, BufMut};
use itertools::Itertools;
use runkv_storage::raft_log_store::entry::RaftLogBatch;
use runkv_storage::raft_log_store::RaftLogStore;

use crate::error::{Error, Result};

const RAFT_HARD_STATE_KEY: &[u8] = b"raft_hard_state";
const RAFT_CONF_STATE_KEY: &[u8] = b"raft_conf_state";

const DEFAULT_RAFT_LOG_ENTRY_CAPACITY: usize = 64;

pub fn encode_entry_data(entry: &raft::prelude::Entry) -> Vec<u8> {
    let mut buf = Vec::with_capacity(DEFAULT_RAFT_LOG_ENTRY_CAPACITY);
    buf.put_i32_le(entry.entry_type);
    buf.put_slice(&entry.data);
    buf
}

pub fn decode_entry_data(mut buf: &[u8]) -> (i32, raft::prelude::EntryType, &[u8]) {
    let raw_entry_type = buf.get_i32_le();
    let entry_type = match raw_entry_type {
        0 => raft::prelude::EntryType::EntryNormal,
        1 => raft::prelude::EntryType::EntryConfChange,
        2 => raft::prelude::EntryType::EntryConfChangeV2,
        _ => unreachable!(),
    };
    (raw_entry_type, entry_type, buf)
}

fn err(e: impl Into<Error>) -> raft::Error {
    raft::Error::Store(raft::StorageError::Other(e.into().into()))
}

#[derive(Clone)]
pub struct RaftGroupLogStore {
    group: u64,
    core: RaftLogStore,
}

impl std::fmt::Debug for RaftGroupLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftGroupLogStore")
            .field("group", &self.group)
            .finish()
    }
}

impl RaftGroupLogStore {
    pub fn new(group: u64, core: RaftLogStore) -> Self {
        Self { group, core }
    }

    pub async fn append(&self, batches: Vec<RaftLogBatch>) -> Result<()> {
        self.core.append(batches).await.map_err(Error::StorageError)
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.core
            .put(self.group, key, value)
            .await
            .map_err(Error::StorageError)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.core
            .get(self.group, key)
            .await
            .map_err(Error::StorageError)
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.core
            .delete(self.group, key)
            .await
            .map_err(Error::StorageError)
    }

    pub async fn put_hard_state(&self, hs: &raft::prelude::HardState) -> Result<()> {
        let value = bincode::serialize(hs).map_err(Error::serde_err)?;
        self.put(RAFT_HARD_STATE_KEY.to_vec(), value).await
    }

    pub async fn get_hard_state(&self) -> Result<Option<raft::prelude::HardState>> {
        let buf = match self.get(RAFT_HARD_STATE_KEY.to_vec()).await? {
            None => return Ok(None),
            Some(buf) => buf,
        };
        let hs = bincode::deserialize(&buf).map_err(Error::serde_err)?;
        Ok(Some(hs))
    }

    pub async fn put_conf_state(&self, cs: &raft::prelude::ConfState) -> Result<()> {
        let value = bincode::serialize(cs).map_err(Error::serde_err)?;
        self.put(RAFT_CONF_STATE_KEY.to_vec(), value).await
    }

    pub async fn get_conf_state(&self) -> Result<Option<raft::prelude::ConfState>> {
        let buf = match self.get(RAFT_CONF_STATE_KEY.to_vec()).await? {
            None => return Ok(None),
            Some(buf) => buf,
        };
        let hs = bincode::deserialize(&buf).map_err(Error::serde_err)?;
        Ok(Some(hs))
    }

    pub async fn entries(&self, index: u64, max_len: usize) -> Result<Vec<raft::prelude::Entry>> {
        let raw_entries = self.core.entries(self.group, index, max_len).await?;
        let entries = raw_entries
            .into_iter()
            .map(|raw_entry| {
                let (raw_entry_type, _, data) = decode_entry_data(&raw_entry.data);
                raft::prelude::Entry {
                    entry_type: raw_entry_type,
                    term: raw_entry.term,
                    index: raw_entry.index,
                    data: data.to_vec(),
                    context: raw_entry.ctx,
                    ..Default::default()
                }
            })
            .collect_vec();
        Ok(entries)
    }
}

#[async_trait]
impl raft::Storage for RaftGroupLogStore {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState`
    /// which contains `HardState` and `ConfState`.
    ///
    /// `RaftState` could be initialized or not. If it's initialized it means the `Storage` is
    /// created with a configuration, and its last index and term should be greater than 0.
    #[tracing::instrument(level = "trace")]
    async fn initial_state(&self) -> raft::Result<raft::RaftState> {
        let hs = self
            .get_hard_state()
            .await
            .map_err(err)?
            .unwrap_or_default();
        let cs = self
            .get_conf_state()
            .await
            .map_err(err)?
            .unwrap_or_default();
        Ok(raft::RaftState {
            hard_state: hs,
            conf_state: cs,
        })
    }

    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// Entries are supported to be fetched asynchorously depending on the context. Async is
    /// optional. Storage should check context.can_async() first and decide whether to fetch
    /// entries asynchorously based on its own implementation. If the entries are fetched
    /// asynchorously, storage should return LogTemporarilyUnavailable, and application needs to
    /// call `on_entries_fetched(context)` to trigger re-fetch of the entries after the storage
    /// finishes fetching the entries.
    ///
    /// # Panics
    ///
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    #[tracing::instrument(level = "trace")]
    async fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: Option<u64>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        let high = match max_size {
            None => high,
            Some(max_size) => std::cmp::min(high, low.saturating_add(std::cmp::max(max_size, 1))),
        };
        let index = low;
        let max_len = (high - low) as usize;
        let raw_entries = self
            .core
            .may_entries(self.group, index, max_len, false)
            .await
            .map_err(err)?;
        let entries = raw_entries
            .into_iter()
            .map(|raw_entry| {
                let (raw_entry_type, _, data) = decode_entry_data(&raw_entry.data);
                raft::prelude::Entry {
                    entry_type: raw_entry_type,
                    term: raw_entry.term,
                    index: raw_entry.index,
                    data: data.to_vec(),
                    context: raw_entry.ctx,
                    ..Default::default()
                }
            })
            .collect_vec();
        Ok(entries)
    }

    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    #[tracing::instrument(level = "trace", ret, err)]
    async fn term(&self, idx: u64) -> raft::Result<u64> {
        let may_term = self.core.term(self.group, idx).await.map_err(err)?;
        if let Some(term) = may_term {
            return Ok(term);
        }
        if idx < self.first_index().await? {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        Err(raft::Error::Store(raft::StorageError::Unavailable))
    }

    /// Returns the index of the first log entry that is possible available via entries, which will
    /// always equal to `truncated index` plus 1.
    ///
    /// New created (but not initialized) `Storage` can be considered as truncated at 0 so that 1
    /// will be returned in this case.
    #[tracing::instrument(level = "trace")]
    async fn first_index(&self) -> raft::Result<u64> {
        self.core.masked_first_index(self.group).await.map_err(err)
    }

    /// The index of the last entry replicated in the `Storage`.
    #[tracing::instrument(level = "trace")]
    async fn last_index(&self) -> raft::Result<u64> {
        self.core.masked_last_index(self.group).await.map_err(err)
    }

    /// Returns the most recent snapshot.
    ///
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    /// A snapshot's index must not less than the `request_index`.
    /// `to` indicates which peer is requesting the snapshot.
    #[tracing::instrument(level = "trace")]
    async fn snapshot(
        &self,
        _request_index: u64,
        _to: u64,
    ) -> raft::Result<raft::prelude::Snapshot> {
        // Impl me!!!
        // Impl me!!!
        // Impl me!!!
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use raft::Storage;
    use runkv_storage::raft_log_store::entry::RaftLogBatchBuilder;
    use runkv_storage::raft_log_store::log::Persist;
    use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    async fn test_clone_safe() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();

        let options = RaftLogStoreOptions {
            node: 0,
            log_dir_path: path.to_string(),
            log_file_capacity: 64 << 20,
            block_cache_capacity: 64 << 20,
            persist: Persist::Sync,
        };
        let raft_log_store = RaftLogStore::open(options).await.unwrap();

        raft_log_store.add_group(1).await.unwrap();

        let raft_node = RaftGroupLogStore::new(1, raft_log_store);
        let raft_node_clone = raft_node.clone();

        let mut builder = RaftLogBatchBuilder::default();
        builder.add(1, 1, 1, &[b'c'; 16], &[b'd'; 16]);
        let batches = builder.build();

        raft_node.append(batches).await.unwrap();

        let l1 = raft_node.last_index().await;
        let l2 = raft_node_clone.last_index().await;
        assert_eq!(l1, l2);
    }
}
