use std::sync::Arc;
use std::time::Instant;

use super::block_cache::BlockCache;
use super::entry::{Compact, Entry as LogEntry, Kv, Mask, RaftLogBatch, Truncate};
use super::log::{Log, LogOptions, Persist};
use super::mem::{EntryIndex, MemStates};
use super::metrics::{RaftLogStoreMetrics, RaftLogStoreMetricsRef};
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Entry {
    pub group: u64,
    pub term: u64,
    pub index: u64,
    pub ctx: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct RaftLogStoreOptions {
    pub node: u64,
    pub log_dir_path: String,
    pub log_file_capacity: usize,
    pub block_cache_capacity: usize,
    pub persist: Persist,
}

struct AppendContext {
    group: u64,
    first_index: u64,
    raw: Vec<u8>,
    data_segment_offset: usize,
    data_segment_len: usize,
    indices: Vec<EntryIndex>,
}

struct RaftLogStoreCore {
    log: Log,
    states: MemStates,
    block_cache: BlockCache,

    metrics: RaftLogStoreMetricsRef,
}

/// [`RaftLogStore`] is designed for storing raft log entries and some small kv pairs from multiple
/// raft groups.
///
/// # Safety
///
/// [`RaftLogStore`] ensure that operations across multiple raft groups are safe. But operations of
/// a same raft group MUST be performed in order.
#[derive(Clone)]
pub struct RaftLogStore {
    node: u64,

    core: Arc<RaftLogStoreCore>,
}

impl std::fmt::Debug for RaftLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftLogStore")
            .field("node", &self.node)
            .finish()
    }
}

impl RaftLogStore {
    pub async fn open(options: RaftLogStoreOptions) -> Result<Self> {
        let states = MemStates::new(options.node);

        let metrics = Arc::new(RaftLogStoreMetrics::new(options.node));

        let log_options = LogOptions {
            node: options.node,
            path: options.log_dir_path,
            log_file_capacity: options.log_file_capacity,
            persist: options.persist,

            metrics: metrics.clone(),
        };

        let log = Log::open(log_options).await?;

        #[futures_async_stream::for_await]
        for item in log.replay() {
            let (file_id, write_offset, entry) = item?;
            match entry {
                LogEntry::RaftLogBatch(batch) => {
                    let (data_segment_offset, data_segment_len) = batch.data_segment_location();
                    let group = batch.group();
                    let term = batch.term();
                    let first_index = batch.first_index();
                    let block_offset = write_offset + data_segment_offset + 1;
                    let block_len = data_segment_len;
                    let mut indices = Vec::with_capacity(batch.len());
                    for i in 0..batch.len() {
                        let (offset, len) = batch.location(i);
                        let index = EntryIndex {
                            term,
                            ctx: batch.ctx(i).to_vec(),
                            file_id,
                            block_offset,
                            block_len,
                            offset,
                            len,
                        };
                        indices.push(index);
                    }
                    states.may_add_group(group).await;
                    states.append(group, first_index, indices).await?;
                }
                LogEntry::Truncate(Truncate { group, index }) => {
                    states.may_add_group(group).await;
                    states.truncate(group, index).await?;
                }
                LogEntry::Compact(Compact { group, index }) => {
                    states.may_add_group(group).await;
                    states.compact(group, index).await?;
                }
                LogEntry::Mask(Mask { group, index }) => {
                    states.may_add_group(group).await;
                    states.mask(group, index).await?;
                }
                LogEntry::Kv(Kv::Put { group, key, value }) => {
                    states.may_add_group(group).await;
                    states.put(group, key, value).await?;
                }
                LogEntry::Kv(Kv::Delete { group, key }) => {
                    states.may_add_group(group).await;
                    states.delete(group, key).await?;
                }
            }
        }

        Ok(Self {
            node: options.node,

            core: Arc::new(RaftLogStoreCore {
                log,
                states,
                block_cache: BlockCache::new(options.block_cache_capacity, metrics.clone()),

                metrics,
            }),
        })
    }

    pub async fn add_group(&self, group: u64) -> Result<()> {
        self.core.states.add_group(group).await
    }

    /// # Safety
    ///
    /// Removed group needs to be guaranteed never be used again.
    pub async fn remove_group(&self, group: u64) -> Result<()> {
        // TODO: Advance GC safe point.
        self.core.states.remove_group(group).await
    }

    /// Append raft log batch to [`RaftLogStore`].
    pub async fn append(&self, batches: Vec<RaftLogBatch>) -> Result<()> {
        let start = Instant::now();

        let mut ctxs = Vec::with_capacity(batches.len());
        let mut entries = Vec::with_capacity(batches.len());

        for mut batch in batches {
            // Initialize entry indices before write log.
            let mut indices = Vec::with_capacity(batch.len());
            let (data_segment_offset, data_segment_len) = batch.data_segment_location();
            let group = batch.group();
            let term = batch.term();
            let first_index = batch.first_index();
            for i in 0..batch.len() {
                let (offset, len) = batch.location(i);
                let index = EntryIndex {
                    term,
                    ctx: batch.ctx(i).to_vec(),
                    file_id: 0,
                    block_offset: 0,
                    block_len: 0,
                    offset,
                    len,
                };
                indices.push(index);
            }

            // Collect write entries.
            let raw = batch.take_raw();
            let entry = LogEntry::RaftLogBatch(batch);

            entries.push(entry);
            ctxs.push(AppendContext {
                group,
                first_index,
                raw,
                data_segment_offset,
                data_segment_len,
                indices,
            });
        }

        // Append log.
        let handles = self.core.log.append(entries).await?;

        for (mut ctx, handle) in ctxs.into_iter().zip(handles.into_iter()) {
            let file_id = handle.file_id;
            let block_offset = handle.offset + ctx.data_segment_offset + 1;
            let block_len = ctx.data_segment_len;
            for index in ctx.indices.iter_mut() {
                index.file_id = file_id;
                index.block_offset = block_offset;
                index.block_len = block_len;
            }

            // Fill block cache.
            self.core
                .block_cache
                .insert(file_id, block_offset, Arc::new(ctx.raw))
                .await;

            // Update memory states.
            self.core
                .states
                .append(ctx.group, ctx.first_index, ctx.indices)
                .await?;
        }

        self.core
            .metrics
            .append_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Truncate raft log of given `group` since given `index`.
    pub async fn truncate(&self, group: u64, index: u64) -> Result<()> {
        self.core
            .log
            .append(vec![LogEntry::Truncate(Truncate { group, index })])
            .await?;
        self.core.states.truncate(group, index).await?;
        Ok(())
    }

    /// Mark all raft log entries before given `index` of the given `group` can be safely deleted.
    pub async fn compact(&self, group: u64, index: u64) -> Result<()> {
        self.core
            .log
            .append(vec![LogEntry::Compact(Compact { group, index })])
            .await?;
        self.core.states.compact(group, index).await?;
        Ok(())
    }

    /// Mask any indices before the given index.
    ///
    /// Masked indices are not deleted from the state, but can only be accessed with `unmask` set to
    /// `true`.
    pub async fn mask(&self, group: u64, index: u64) -> Result<()> {
        self.core
            .log
            .append(vec![LogEntry::Mask(Mask { group, index })])
            .await?;
        self.core.states.mask(group, index).await?;
        Ok(())
    }

    /// Get raft log entries from [`RaftLogStore`].
    ///
    /// Reeturns empty when given `index` is not valid.
    pub async fn may_entries(
        &self,
        group: u64,
        index: u64,
        max_len: usize,
        unmask: bool,
    ) -> Result<Vec<Entry>> {
        let (first_index, indices) = self
            .core
            .states
            .may_entries(group, index, max_len, unmask)
            .await?;
        let mut entries = Vec::with_capacity(indices.len());
        for (i, ei) in indices.into_iter().enumerate() {
            let data = self.entry_data(&ei).await?;
            let entry = Entry {
                group,
                term: ei.term,
                index: first_index + i as u64,
                ctx: ei.ctx,
                data,
            };
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Get raft log entries from [`RaftLogStore`].
    ///
    /// Returns `Err` when given `index` is not valid.
    pub async fn entries(&self, group: u64, index: u64, max_len: usize) -> Result<Vec<Entry>> {
        let indices = self.core.states.entries(group, index, max_len).await?;
        let mut entries = Vec::with_capacity(indices.len());
        for (i, ei) in indices.into_iter().enumerate() {
            let data = self.entry_data(&ei).await?;
            let entry = Entry {
                group,
                term: ei.term,
                index: index + i as u64,
                ctx: ei.ctx,
                data,
            };
            entries.push(entry);
        }
        Ok(entries)
    }

    pub async fn term(&self, group: u64, index: u64) -> Result<Option<u64>> {
        self.core.states.term(group, index).await
    }

    pub async fn ctx(&self, group: u64, index: u64) -> Result<Option<Vec<u8>>> {
        self.core.states.ctx(group, index).await
    }

    pub async fn first_index(&self, group: u64) -> Result<u64> {
        self.core.states.first_index(group).await
    }

    pub async fn last_index(&self, group: u64) -> Result<u64> {
        self.core.states.last_index(group).await
    }

    pub async fn masked_first_index(&self, group: u64) -> Result<u64> {
        self.core.states.masked_first_index(group).await
    }

    pub async fn masked_last_index(&self, group: u64) -> Result<u64> {
        self.core.states.masked_last_index(group).await
    }

    pub async fn put(&self, group: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        #[cfg(feature = "deadlock")]
        tracing::info!("{} logappend enter", group);
        self.core
            .log
            .append(vec![LogEntry::Kv(Kv::Put {
                group,
                key: key.clone(),
                value: value.clone(),
            })])
            .await?;
        #[cfg(feature = "deadlock")]
        tracing::info!("{} logappend exit", group);
        #[cfg(feature = "deadlock")]
        tracing::info!("{} stateput enter", group);
        self.core.states.put(group, key, value).await?;
        #[cfg(feature = "deadlock")]
        tracing::info!("{} stateput exit", group);
        Ok(())
    }

    pub async fn delete(&self, group: u64, key: Vec<u8>) -> Result<()> {
        self.core
            .log
            .append(vec![LogEntry::Kv(Kv::Delete {
                group,
                key: key.clone(),
            })])
            .await?;
        self.core.states.delete(group, key).await?;
        Ok(())
    }

    pub async fn get(&self, group: u64, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.core.states.get(group, key).await
    }
}

impl RaftLogStore {
    #[tracing::instrument(level = "trace")]
    async fn entry_data(&self, index: &EntryIndex) -> Result<Vec<u8>> {
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
            .get_or_insert_with(index.file_id, index.block_offset, read_file)
            .await?;

        Ok((&block[index.offset..index.offset + index.len]).to_vec())
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use test_log::test;

    use super::*;
    use crate::raft_log_store::entry::RaftLogBatchBuilder;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<RaftLogStore>()
    }

    #[test(tokio::test)]
    async fn test_raft_log_with_cache() {
        test_raft_log(true).await;
    }

    #[test(tokio::test)]
    async fn test_raft_log_without_cache() {
        test_raft_log(false).await;
    }

    async fn test_raft_log(with_cache: bool) {
        // Prepare data.
        let mut dataset = vec![];

        for r#loop in 0..4 {
            let mut builder = RaftLogBatchBuilder::default();
            for group in 1..=4 {
                let base = r#loop * 16;
                for index in base + 1..=base + 16 {
                    builder.add(group, 1, index, b"some-ctx", &data(group, 1, index));
                }
            }
            let batches = builder.build();
            assert_eq!(batches.len(), 4);
            dataset.push(batches);
        }

        let tempdir = tempfile::tempdir().unwrap();
        let options = RaftLogStoreOptions {
            node: 0,
            log_dir_path: tempdir.path().to_str().unwrap().to_string(),
            // Make sure each loop can exceed the capacity.
            log_file_capacity: 256,
            block_cache_capacity: if with_cache { 1024 } else { 0 },
            persist: Persist::Sync,
        };

        let store = RaftLogStore::open(options.clone()).await.unwrap();
        store.add_group(1).await.unwrap();
        store.add_group(2).await.unwrap();
        store.add_group(3).await.unwrap();
        store.add_group(4).await.unwrap();
        for batches in dataset {
            store.append(batches).await.unwrap();
        }
        assert_eq!(store.core.log.frozen_file_count().await, 4);
        for group in 1..=4 {
            let entries = store.entries(group, 1, usize::MAX).await.unwrap();
            assert_eq!(
                entries.into_iter().map(|entry| entry.data).collect_vec(),
                (1..=64)
                    .into_iter()
                    .map(|index| data(group, 1, index))
                    .collect_vec()
            );
        }

        drop(store);
        let store = RaftLogStore::open(options.clone()).await.unwrap();
        assert_eq!(store.core.log.frozen_file_count().await, 5);
        for group in 1..=4 {
            let entries = store.entries(group, 1, usize::MAX).await.unwrap();
            assert_eq!(
                entries.into_iter().map(|entry| entry.data).collect_vec(),
                (1..=64)
                    .into_iter()
                    .map(|index| data(group, 1, index))
                    .collect_vec()
            );
        }

        for group in 1..=4 {
            store.compact(group, 9).await.unwrap();
        }
        for group in 1..=4 {
            assert!(store.entries(group, 8, usize::MAX).await.is_err());
            let entries = store.entries(group, 9, usize::MAX).await.unwrap();
            assert_eq!(
                entries.into_iter().map(|entry| entry.data).collect_vec(),
                (9..=64)
                    .into_iter()
                    .map(|index| data(group, 1, index))
                    .collect_vec()
            );
        }

        drop(store);
        let store = RaftLogStore::open(options.clone()).await.unwrap();
        assert_eq!(store.core.log.frozen_file_count().await, 6);
        for group in 1..=4 {
            assert!(store.entries(group, 8, usize::MAX).await.is_err());
            let entries = store.entries(group, 9, usize::MAX).await.unwrap();
            assert_eq!(
                entries.into_iter().map(|entry| entry.data).collect_vec(),
                (9..=64)
                    .into_iter()
                    .map(|index| data(group, 1, index))
                    .collect_vec()
            );
        }

        for group in 1..=4 {
            store.truncate(group, 11).await.unwrap();
        }

        for group in 1..=4 {
            assert!(store.entries(group, 8, usize::MAX).await.is_err());
            let entries = store.entries(group, 9, usize::MAX).await.unwrap();
            assert_eq!(
                entries.into_iter().map(|entry| entry.data).collect_vec(),
                (9..=10)
                    .into_iter()
                    .map(|index| data(group, 1, index))
                    .collect_vec()
            );
        }

        drop(store);
        let store = RaftLogStore::open(options.clone()).await.unwrap();
        assert_eq!(store.core.log.frozen_file_count().await, 7);
        for group in 1..=4 {
            assert!(store.entries(group, 8, usize::MAX).await.is_err());
            let entries = store.entries(group, 9, usize::MAX).await.unwrap();
            assert_eq!(
                entries.into_iter().map(|entry| entry.data).collect_vec(),
                (9..=10)
                    .into_iter()
                    .map(|index| data(group, 1, index))
                    .collect_vec()
            );
        }
    }

    #[test(tokio::test)]
    async fn test_kv_with_cache() {
        test_kv(true).await;
    }

    #[test(tokio::test)]
    async fn test_kv_without_cache() {
        test_kv(false).await;
    }

    async fn test_kv(with_cache: bool) {
        let tempdir = tempfile::tempdir().unwrap();
        let options = RaftLogStoreOptions {
            node: 0,
            log_dir_path: tempdir.path().to_str().unwrap().to_string(),
            // Make sure each loop can exceed the capacity.
            log_file_capacity: 100,
            block_cache_capacity: if with_cache { 1024 } else { 0 },
            persist: Persist::Sync,
        };

        let store = RaftLogStore::open(options.clone()).await.unwrap();
        store.add_group(1).await.unwrap();
        store.add_group(2).await.unwrap();
        store.add_group(3).await.unwrap();
        store.add_group(4).await.unwrap();

        for group in 1..=4 {
            store
                .put(group, b"k1".to_vec(), b"v1".to_vec())
                .await
                .unwrap();
        }

        for group in 1..=4 {
            assert_eq!(
                store.get(group, b"k1".to_vec()).await.unwrap(),
                Some(b"v1".to_vec())
            );
        }

        for group in 1..=4 {
            store
                .put(group, b"k1".to_vec(), b"v2".to_vec())
                .await
                .unwrap();
        }

        for group in 1..=4 {
            assert_eq!(
                store.get(group, b"k1".to_vec()).await.unwrap(),
                Some(b"v2".to_vec())
            );
        }

        drop(store);
        let store = RaftLogStore::open(options.clone()).await.unwrap();
        for group in 1..=4 {
            assert_eq!(
                store.get(group, b"k1".to_vec()).await.unwrap(),
                Some(b"v2".to_vec())
            );
        }

        for group in 1..=4 {
            store.delete(group, b"k1".to_vec()).await.unwrap();
        }

        for group in 1..=4 {
            assert_eq!(store.get(group, b"k1".to_vec()).await.unwrap(), None,);
        }

        drop(store);
        let store = RaftLogStore::open(options.clone()).await.unwrap();
        for group in 1..=4 {
            assert_eq!(store.get(group, b"k1".to_vec()).await.unwrap(), None,);
        }
    }

    fn data(group: u64, term: u64, index: u64) -> Vec<u8> {
        format!("{:15}-{:15}-{:32}", group, term, index).into()
    }
}
