use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures_async_stream::try_stream;
use itertools::Itertools;
use runkv_common::packer::{Item, Packer};
use tokio::fs::{create_dir_all, read_dir};
use tokio::sync::{oneshot, RwLock};
use tracing::{trace, trace_span, Instrument};

use super::entry::Entry;
use super::error::RaftLogStoreError;
use super::file::{ActiveFile, FrozenFile};
use super::metrics::RaftLogStoreMetricsRef;
use crate::error::{Error, Result};
use crate::raft_log_store::file::sync_dir;

const DEFAULT_BUFFER_SIZE: usize = 64 << 10;

#[derive(Clone, Copy, Debug)]
pub enum Persist {
    Flush,
    Sync,
    None,
}

impl FromStr for Persist {
    type Err = Error;

    fn from_str(s: &str) -> core::result::Result<Self, Self::Err> {
        match s {
            "sync" => Ok(Self::Sync),
            "flush" => Ok(Self::Flush),
            "none" => Ok(Self::None),
            _ => Err(Error::Other(format!("fail to parse {} to Persist", s))),
        }
    }
}

fn filename(id: u64) -> String {
    format!("{:08}", id)
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct WriteHandle {
    pub file_id: u64,
    pub offset: usize,
    pub len: usize,
}

#[derive(Clone)]
pub struct LogOptions {
    pub node: u64,
    pub path: String,
    pub log_file_capacity: usize,
    pub persist: Persist,

    pub metrics: RaftLogStoreMetricsRef,
}

struct LogCore {
    active_file: RwLock<ActiveFile>,
    frozen_files: RwLock<Vec<FrozenFile>>,
    first_log_file_id: AtomicU64,

    writer_packer: Packer<Vec<Entry>, Vec<WriteHandle>>,
}

#[derive(Clone)]
pub struct Log {
    node: u64,
    path: String,
    log_file_capacity: usize,
    persist: Persist,

    core: Arc<LogCore>,

    metrics: RaftLogStoreMetricsRef,
}

impl std::fmt::Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Log").field("node", &self.node).finish()
    }
}

impl Log {
    #[tracing::instrument(level = "trace", skip(options))]
    pub async fn open(options: LogOptions) -> Result<Self> {
        create_dir_all(&options.path).await?;
        let (frozen_files, first_log_file_id) = {
            let mut frozen_files = vec![];
            let mut r = read_dir(&options.path).await?;
            while let Some(entry) = r.next_entry().await? {
                let raw_filename = entry.file_name();
                let id = raw_filename
                    .to_str()
                    .ok_or_else(|| {
                        RaftLogStoreError::Other(format!("invalid file name: {:?}", raw_filename))
                    })?
                    .parse()
                    .map_err(|_| {
                        RaftLogStoreError::Other(format!("invalid file name: {:?}", raw_filename))
                    })?;

                let frozen_file = FrozenFile::open(&options.path, id).await?;
                frozen_files.push((id, frozen_file));
            }
            if frozen_files.is_empty() {
                (vec![], 1)
            } else {
                frozen_files.sort_by_key(|(id, _)| *id);
                let first_log_file_id = frozen_files[0].0;
                for (i, frozen_file) in frozen_files.iter().enumerate() {
                    if frozen_file.0 != first_log_file_id + i as u64 {
                        return Err(RaftLogStoreError::Other(format!(
                            "log file {} is missing",
                            first_log_file_id + i as u64
                        ))
                        .into());
                    }
                }
                (
                    frozen_files.into_iter().map(|(_, file)| file).collect_vec(),
                    first_log_file_id,
                )
            }
        };
        let active_file_id = first_log_file_id + frozen_files.len() as u64;
        let active_file = ActiveFile::open(&options.path, active_file_id).await?;

        Ok(Self {
            node: options.node,
            path: options.path,
            log_file_capacity: options.log_file_capacity,
            persist: options.persist,
            core: Arc::new(LogCore {
                active_file: RwLock::new(active_file),
                frozen_files: RwLock::new(frozen_files),
                first_log_file_id: AtomicU64::new(first_log_file_id),

                writer_packer: Packer::new(64),
            }),

            metrics: options.metrics,
        })
    }

    #[tracing::instrument(level = "trace", ret)]
    pub async fn frozen_file_count(&self) -> usize {
        self.core.frozen_files.read().await.len()
    }

    /// Append [`entries`] to log file.
    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn append(&self, entries: Vec<Entry>) -> Result<Vec<WriteHandle>> {
        let start = Instant::now();
        let mut total_size = 0;

        let (tx, rx) = oneshot::channel();
        // Append entries to queue.
        let is_leader = self.core.writer_packer.append(entries, Some(tx));

        if is_leader {
            // Get exclusive lock to prevent next batch write concurrently.
            let mut active_file = self
                .core
                .active_file
                .write()
                .instrument(trace_span!("wait_queue"))
                .await;
            let mut offset = active_file.offset().await as usize;

            let mut buf = Vec::with_capacity(DEFAULT_BUFFER_SIZE);

            // Take writer batch.
            let writers = self.core.writer_packer.package();
            self.metrics
                .batch_writers_histogram
                .observe(writers.len() as f64);

            let mut txs = Vec::with_capacity(writers.len());
            let mut handles = Vec::with_capacity(writers.len());

            for Item {
                data: entries,
                notifier,
            } in writers
            {
                let mut entry_handles = Vec::with_capacity(entries.len());

                let file_id = self.core.first_log_file_id.load(Ordering::Acquire)
                    + self.core.frozen_files.read().await.len() as u64;

                for entry in entries {
                    let entry_offset = offset + buf.len();
                    entry.encode(&mut buf);
                    let entry_len = offset + buf.len() - entry_offset;
                    total_size += entry_len;
                    entry_handles.push(WriteHandle {
                        file_id,
                        offset: entry_offset,
                        len: entry_len,
                    });
                }

                if offset + buf.len() >= self.log_file_capacity {
                    // Force write buffer.
                    active_file.append(&buf).await?;
                    self.metrics.sync_size_histogram.observe(buf.len() as f64);
                    buf.clear();

                    // Sync old active file.
                    let now = Instant::now();
                    active_file.sync_all().await?;
                    self.metrics
                        .sync_latency_histogram
                        .observe(now.elapsed().as_secs_f64());

                    // Rotate active file.
                    let new_active_file_id = file_id + 1;
                    *active_file = ActiveFile::open(&self.path, new_active_file_id).await?;
                    offset = 0;

                    // Sync dir.
                    let start_sync = Instant::now();
                    sync_dir(&self.path).await?;
                    self.metrics
                        .sync_latency_histogram
                        .observe(start_sync.elapsed().as_secs_f64());

                    trace!(
                        "rotate log from {} to {}",
                        filename(file_id),
                        filename(new_active_file_id)
                    );

                    // Add old active file to frozen file list.
                    let frozen_file = FrozenFile::open(&self.path, file_id).await?;
                    self.core.frozen_files.write().await.push(frozen_file);
                }

                txs.push(notifier.unwrap());
                handles.push(entry_handles);
            }

            if !buf.is_empty() {
                active_file.append(&buf).await?;
                self.metrics.sync_size_histogram.observe(buf.len() as f64);
                buf.clear();

                let start_sync = Instant::now();
                match self.persist {
                    Persist::Flush => {
                        active_file.flush().await?;
                    }
                    Persist::Sync => {
                        active_file.sync_data().await?;
                    }
                    _ => {}
                }
                self.metrics
                    .sync_latency_histogram
                    .observe(start_sync.elapsed().as_secs_f64());
            }

            for (tx, handle) in txs.into_iter().zip(handles.into_iter()) {
                tx.send(handle).map_err(|handle| {
                    Error::Other(format!("failed to send write handle: {:?}", handle))
                })?;
            }
            drop(active_file);
        }

        let handles = rx.await.map_err(Error::err)?;

        self.metrics
            .append_log_latency_histogram
            .observe(start.elapsed().as_secs_f64());
        self.metrics
            .append_log_throughput_guage
            .add(total_size as f64);

        Ok(handles)
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn read(
        &self,
        log_file_id: u64,
        block_offset: u64,
        block_len: usize,
    ) -> Result<Vec<u8>> {
        // NOTE: Be careful with the lock order between `active_file` and `frozen_files`!!!

        enum File {
            Active(ActiveFile),
            Frozen(FrozenFile),
        }

        let file = {
            let frozen_files = self.core.frozen_files.read().await;
            let first_log_file_id = self.core.first_log_file_id.load(Ordering::Acquire);
            let index = (log_file_id - first_log_file_id) as usize;
            if index < frozen_files.len() {
                File::Frozen(frozen_files[index].clone())
            } else {
                drop(frozen_files);
                let active_file = self.core.active_file.read().await.clone();
                // FIXME: A workaround here. Active file rotation and frozen file vec mutation
                // should be combined.
                if active_file.id() == log_file_id {
                    File::Active(active_file)
                } else {
                    let frozen_files = self.core.frozen_files.read().await;
                    let first_log_file_id = self.core.first_log_file_id.load(Ordering::Acquire);
                    let index = (log_file_id - first_log_file_id) as usize;
                    assert!(
                        index < frozen_files.len(),
                        "index: {}, frozen_file len: {}",
                        index,
                        frozen_files.len(),
                    );
                    File::Frozen(frozen_files[index].clone())
                }
            }
        };

        let buf = match file {
            File::Active(file) => {
                assert_eq!(file.id(), log_file_id);
                file.read(block_offset, block_len).await?
            }
            File::Frozen(file) => {
                assert_eq!(file.id(), log_file_id);
                file.read(block_offset, block_len).await?
            }
        };
        Ok(buf)
    }

    /// Yield [`(file id, offset, Entry)`] of all frozen logs in order.
    // TODO: Remove clippy exception. Currently clippy reports `needless_lifetime` with
    // `try_stream`.
    #[allow(clippy::needless_lifetimes)]
    #[try_stream(ok = (u64,usize, Entry), error = RaftLogStoreError)]
    pub async fn replay(&self) {
        let frozen_files = self.core.frozen_files.read().await;

        let begin_log_file_id = self.core.first_log_file_id.load(Ordering::Acquire);
        let end_log_file_id = begin_log_file_id + frozen_files.len() as u64;

        for (i, current_log_file_id) in (begin_log_file_id..end_log_file_id).enumerate() {
            trace!("replay index: {} file id: {}", i, current_log_file_id);
            let buf = frozen_files[i].read_all().await.unwrap();
            let cursor = &mut &buf[..];
            while !cursor.is_empty() {
                let offset = buf.len() - cursor.len();
                let entry = Entry::decode(cursor);
                yield (current_log_file_id, offset, entry);
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.core.active_file.read().await.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::*;
    use crate::raft_log_store::entry::RaftLogBatchBuilder;
    use crate::raft_log_store::metrics::RaftLogStoreMetrics;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<Log>()
    }

    #[test(tokio::test)]
    async fn test_pipe_log_recovery() {
        let tempdir = tempfile::tempdir().unwrap();
        let options = LogOptions {
            node: 1,
            path: tempdir.path().to_str().unwrap().to_string(),
            // Estimated size of each compressed entry is 111.
            log_file_capacity: 100,
            persist: Persist::Sync,
            metrics: Arc::new(RaftLogStoreMetrics::new(0)),
        };
        let log = Log::open(options.clone()).await.unwrap();
        let entries = generate_entries(4, 16, vec![b'x'; 64]);
        assert_eq!(entries.len(), 4);

        for entry in entries.iter().cloned() {
            log.append(vec![entry]).await.unwrap();
        }
        assert_eq!(log.frozen_file_count().await, 4);
        let mut buf = vec![];
        for i in 0..4 {
            buf.append(
                &mut log.core.frozen_files.read().await[i]
                    .read_all()
                    .await
                    .unwrap(),
            );
        }
        let mut buf = &buf[..];
        let decoded_entries = (0..4)
            .into_iter()
            .map(|_| Entry::decode(&mut buf))
            .collect_vec();
        assert_eq!(decoded_entries, entries);
        log.close().await.unwrap();

        // Recover pipe log.
        drop(log);
        let log = Log::open(options).await.unwrap();
        assert_eq!(log.frozen_file_count().await, 5);
        let mut buf = vec![];
        for i in 0..4 {
            buf.append(
                &mut log.core.frozen_files.read().await[i]
                    .read_all()
                    .await
                    .unwrap(),
            );
        }
        let mut buf = &buf[..];
        let decoded_entries = (0..4)
            .into_iter()
            .map(|_| Entry::decode(&mut buf))
            .collect_vec();
        assert_eq!(decoded_entries, entries);
    }

    fn generate_entries(groups: usize, group_size: usize, data: Vec<u8>) -> Vec<Entry> {
        let mut builder = RaftLogBatchBuilder::default();

        for group in 1..=groups as u64 {
            let term = 1;
            for index in 1..=group_size as u64 {
                builder.add(group, term, index, b"some-ctx", &data);
            }
        }
        let batches = builder.build();
        let batches = batches.into_iter().map(|batch| batch.into()).collect_vec();
        assert_eq!(batches.len(), groups);
        batches
    }
}
