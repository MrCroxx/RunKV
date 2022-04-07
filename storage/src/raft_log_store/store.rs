use std::sync::Arc;

use futures_async_stream::for_await;
use itertools::Itertools;
use tracing::trace;

use super::block_cache::BlockCache;
use super::entry::{Entry, RaftLogBatch};
use super::log::{Log, LogOptions, LogRef};
use super::mem::{EntryIndex, MemStates};
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct RaftLogStoreOptions {
    pub log_dir_path: String,
    pub log_file_capacity: usize,
    pub block_cache_capacity: usize,
}

struct RaftLogStoreCore {
    log: LogRef,
    states: MemStates,
    block_cache: BlockCache,
}

#[derive(Clone)]
pub struct RaftLogStore {
    core: Arc<RaftLogStoreCore>,
}

impl RaftLogStore {
    pub async fn open(options: RaftLogStoreOptions) -> Result<Self> {
        let states = MemStates::default();

        let log_options = LogOptions {
            path: options.log_dir_path,
            log_file_capacity: options.log_file_capacity,
        };

        let log = Log::open(log_options).await?;

        #[for_await]
        for item in log.replay() {
            let (file_id, write_offset, entry) = item?;
            match entry {
                Entry::RaftLogBatch(batch) => {
                    let (data_segment_offset, data_segment_len) = batch.data_segment_location();
                    let group = batch.group();
                    let term = batch.term();
                    let first_index = batch.first_index();
                    let locations = (0..batch.len())
                        .into_iter()
                        .map(|i| batch.location(i))
                        .collect_vec();
                    let indices = locations
                        .into_iter()
                        .map(|(offset, len)| EntryIndex {
                            term,
                            file_id,
                            block_offset: write_offset + data_segment_offset + 1, // `1` for entry type
                            block_len: data_segment_len,
                            offset,
                            len,
                        })
                        .collect_vec();
                    states.may_add_group(group, first_index).await;
                    states.append(group, first_index, indices).await?;
                }
                Entry::Compact(_) => todo!(),
                Entry::Kv(_) => todo!(),
            }
        }

        let log = Arc::new(log);

        Ok(Self {
            core: Arc::new(RaftLogStoreCore {
                log,
                states,
                block_cache: BlockCache::new(options.block_cache_capacity),
            }),
        })
    }

    pub async fn add_group(&self, group: u64) -> Result<()> {
        self.core.states.add_group(group, 1).await
    }

    /// # Safety
    ///
    /// Removed group needs to be guaranteed never be used again.
    pub async fn remove_group(&self, group: u64) -> Result<()> {
        // TODO: Advance GC safe point.
        self.core.states.remove_group(group).await
    }

    /// Append raft log batch to [`RaftLogStore`].
    pub async fn append(&self, batch: RaftLogBatch) -> Result<()> {
        let (data_segment_offset, data_segment_len) = batch.data_segment_location();
        let group = batch.group();
        let term = batch.term();
        let first_index = batch.first_index();
        let locations = (0..batch.len())
            .into_iter()
            .map(|i| batch.location(i))
            .collect_vec();

        let entry = Entry::RaftLogBatch(batch);
        let (file_id, write_offset, _write_len) = self.core.log.push(entry).await?;

        let indices = locations
            .into_iter()
            .map(|(offset, len)| EntryIndex {
                term,
                file_id,
                block_offset: write_offset + data_segment_offset + 1, // `1` for entry type
                block_len: data_segment_len,
                offset,
                len,
            })
            .collect_vec();

        self.core.states.append(group, first_index, indices).await?;

        Ok(())
    }

    /// Get raft log entries from [`RaftLogStore`].
    pub async fn entries(&self, group: u64, index: u64, max_len: usize) -> Result<Vec<Vec<u8>>> {
        let indices = self.core.states.entries(group, index, max_len).await?;
        // TODO: Use concurrent operation?
        let mut entries = Vec::with_capacity(indices.len());
        for index in indices {
            let entry = self.entry(index).await?;
            entries.push(entry);
        }
        Ok(entries)
    }

    pub async fn put(&self) {
        todo!()
    }

    pub async fn delete(&self) {
        todo!()
    }

    pub async fn get(&self) {
        todo!()
    }
}

impl RaftLogStore {
    async fn entry(&self, index: EntryIndex) -> Result<Vec<u8>> {
        trace!("read entry: {:?}", index);
        let log = self.core.log.clone();
        let index_clone = index.clone();
        let read_file = async move {
            let raw = log
                .read(
                    index_clone.file_id,
                    index_clone.block_offset as u64,
                    index_clone.block_len,
                )
                .await?;
            let block = RaftLogBatch::extract_data_segment(&raw)?;
            Ok(Arc::new(block))
        };

        let block = self
            .core
            .block_cache
            .get_or_insert_with(index.file_id, index.offset, read_file)
            .await?;

        Ok((&block[index.offset..index.offset + index.len]).to_vec())
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;
    use crate::raft_log_store::entry::RaftLogBatchBuilder;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<RaftLogStore>()
    }

    #[test(tokio::test)]
    async fn test_raft_log() {
        // Prepare data.
        let mut builder = RaftLogBatchBuilder::default();
        for group in 1..=4 {
            for index in 1..=16 {
                builder.add(group, 1, index, &data(1, index));
            }
        }
        let batches = builder.build();
        assert_eq!(batches.len(), 4);

        let tempdir = tempfile::tempdir().unwrap();
        let options = RaftLogStoreOptions {
            log_dir_path: tempdir.path().to_str().unwrap().to_string(),
            // Estimated size of each compressed entry is 111.
            log_file_capacity: 100,
            block_cache_capacity: 1024,
        };

        let store = RaftLogStore::open(options.clone()).await.unwrap();
        store.add_group(1).await.unwrap();
        store.add_group(2).await.unwrap();
        store.add_group(3).await.unwrap();
        store.add_group(4).await.unwrap();
        for batch in batches {
            store.append(batch).await.unwrap();
        }
        assert_eq!(store.core.log.frozen_file_count().await, 4);
        for group in 1..=4 {
            let entries = store.entries(group, 1, usize::MAX).await.unwrap();
            assert_eq!(
                entries,
                (1..=16)
                    .into_iter()
                    .map(|index| data(1, index))
                    .collect_vec()
            );
        }

        drop(store);
        let store = RaftLogStore::open(options.clone()).await.unwrap();
        assert_eq!(store.core.log.frozen_file_count().await, 5);
        for group in 1..=4 {
            let entries = store.entries(group, 1, usize::MAX).await.unwrap();
            assert_eq!(
                entries,
                (1..=16)
                    .into_iter()
                    .map(|index| data(1, index))
                    .collect_vec()
            );
        }
    }

    fn data(term: u64, index: u64) -> Vec<u8> {
        format!("{:31}-{:32}", term, index).into()
    }
}
