use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::channel::oneshot;
use futures_async_stream::try_stream;
use itertools::Itertools;
use parking_lot::RwLock;
use tokio::fs::{create_dir_all, read_dir, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};
use tracing::trace;

use super::entry::Entry;
use super::error::RaftLogStoreError;
use super::metrics::RaftLogStoreMetricsRef;
use super::DEFAULT_LOG_BATCH_SIZE;
use crate::error::{Error, Result};

const DEFAULT_BUFFER_SIZE: usize = 64 << 10;

fn filename(id: u64) -> String {
    format!("{:08}", id)
}

async fn new_active_file(path: impl AsRef<Path>, active_file_id: u64) -> Result<File> {
    let mut active_file_open_options = OpenOptions::new();
    active_file_open_options.create(true);
    active_file_open_options.read(true);
    active_file_open_options.append(true);
    let file = active_file_open_options
        .open(path.as_ref().join(filename(active_file_id)))
        .await?;
    Ok(file)
}

#[derive(Clone, Debug)]
pub struct WriteHandle {
    pub file_id: u64,
    pub offset: usize,
    pub len: usize,
}

struct Writer {
    entries: Vec<Entry>,
    tx: oneshot::Sender<WriteHandle>,
}

impl Writer {
    fn new(entries: Vec<Entry>, tx: oneshot::Sender<WriteHandle>) -> Self {
        Self { entries, tx }
    }
}

#[derive(Clone)]
pub struct LogOptions {
    pub path: String,
    pub log_file_capacity: usize,

    pub metrics: RaftLogStoreMetricsRef,
}

struct LogCore {
    active_file: Arc<AsyncRwLock<File>>,
    frozen_files: Arc<RwLock<Vec<File>>>,
    first_log_file_id: AtomicU64,

    queue: Arc<RwLock<Vec<Writer>>>,
}

#[derive(Clone)]
pub struct Log {
    path: String,
    log_file_capacity: usize,

    core: Arc<LogCore>,

    metrics: RaftLogStoreMetricsRef,
}

impl Log {
    pub async fn open(options: LogOptions) -> Result<Self> {
        create_dir_all(&options.path).await?;
        let (frozen_files, first_log_file_id) = {
            let mut frozen_files = vec![];
            let mut r = read_dir(&options.path).await?;
            while let Some(entry) = r.next_entry().await? {
                let file = File::open(entry.path()).await?;
                frozen_files.push((
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|s| {
                            RaftLogStoreError::Other(format!("invalid file name: {:?}", s))
                        })?
                        .parse::<u64>()
                        .map_err(|e| {
                            RaftLogStoreError::Other(format!("invalid file name: {}", e))
                        })?,
                    file,
                ));
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
        let active_file = new_active_file(&options.path, active_file_id).await?;

        Ok(Self {
            path: options.path,
            log_file_capacity: options.log_file_capacity,
            core: Arc::new(LogCore {
                active_file: Arc::new(AsyncRwLock::new(active_file)),
                frozen_files: Arc::new(RwLock::new(frozen_files)),
                first_log_file_id: AtomicU64::new(first_log_file_id),
                queue: Arc::new(RwLock::new(vec![])),
            }),

            metrics: options.metrics,
        })
    }

    /// Append [`entries`] to log file.
    pub async fn append(&self, entries: Vec<Entry>) -> Result<WriteHandle> {
        let (tx, rx) = oneshot::channel();
        let writer = Writer::new(entries, tx);
        // Append entries to queue.
        let is_leader = {
            let mut queue = self.core.queue.write();
            let is_leader = queue.is_empty();
            queue.push(writer);
            is_leader
        };

        if is_leader {
            // Take writer batch.
            let writers = {
                let mut queue = self.core.queue.write();
                std::mem::take(&mut (*queue))
            };

            let mut sync_size = 0;
            let mut txs = Vec::with_capacity(writers.len());
            let mut handles = Vec::with_capacity(writers.len());

            for writer in writers {
                let mut file = self.core.active_file.write().await;

                let file_id = self.core.first_log_file_id.load(Ordering::Acquire)
                    + self.core.frozen_files.read().len() as u64;
                let offset = file.metadata().await?.len() as usize;
                let mut buf = Vec::with_capacity(DEFAULT_BUFFER_SIZE);

                let mut len = 0;
                for entry in writer.entries {
                    len += entry.encode(&mut buf);
                }
                file.write_all(&buf).await?;
                sync_size += len;

                if offset + len >= self.log_file_capacity {
                    // Sync old active file.
                    let now = Instant::now();
                    file.sync_all().await?;
                    self.metrics
                        .sync_duration_histogram
                        .observe(now.elapsed().as_secs_f64());
                    self.metrics.sync_bytes_guage.add(sync_size as f64);
                    sync_size = 0;

                    // Rotate active file.
                    let new_active_file_id = file_id + 1;
                    *file = new_active_file(&self.path, new_active_file_id).await?;

                    // Sync dir.
                    let now = Instant::now();
                    File::open(&self.path).await?.sync_all().await?;
                    self.metrics
                        .sync_duration_histogram
                        .observe(now.elapsed().as_secs_f64());
                    // Not recording sync dir bytes.
                    // self.metrics.sync_bytes_guage.add(0.0);

                    trace!(
                        "rotate log from {} to {}",
                        filename(file_id),
                        filename(new_active_file_id)
                    );

                    // Add old active file to frozen file list.
                    let frozen_file =
                        File::open(Path::new(&self.path).join(filename(file_id))).await?;
                    self.core.frozen_files.write().push(frozen_file);
                }

                txs.push(writer.tx);
                handles.push(WriteHandle {
                    file_id,
                    offset,
                    len,
                });
            }

            if sync_size > 0 {
                let now = Instant::now();
                self.core.active_file.write().await.sync_data().await?;
                self.metrics
                    .sync_duration_histogram
                    .observe(now.elapsed().as_secs_f64());
                self.metrics.sync_bytes_guage.add(sync_size as f64);
            }

            for (tx, handle) in txs.into_iter().zip(handles.into_iter()) {
                tx.send(handle).map_err(|handle| {
                    Error::Other(format!("failed to send write handle: {:?}", handle))
                })?;
            }
        }

        let handle = rx.await.map_err(Error::err)?;

        Ok(handle)
    }

    pub async fn close(&self) -> Result<()> {
        self.core.active_file.read().await.sync_all().await?;
        Ok(())
    }

    /// Yield [`(file id, offset, Entry)`] of all frozen logs in order.
    // TODO: Remove clippy exception. Currently clippy reports `needless_lifetime` with
    // `try_stream`.
    #[allow(clippy::needless_lifetimes)]
    #[try_stream(ok = (u64,usize, Entry), error = RaftLogStoreError)]
    pub async fn replay(&self) {
        let mut frozen_files = self.core.frozen_files.write();

        let begin_log_file_id = self.core.first_log_file_id.load(Ordering::Acquire);
        let end_log_file_id = begin_log_file_id + frozen_files.len() as u64;

        let mut buf = Vec::with_capacity(DEFAULT_LOG_BATCH_SIZE);
        for (i, current_log_file_id) in (begin_log_file_id..end_log_file_id).enumerate() {
            trace!("replay index: {} file id: {}", i, current_log_file_id);
            frozen_files[i].seek(std::io::SeekFrom::Start(0)).await?;
            buf.clear();
            frozen_files[i].read_to_end(&mut buf).await?;
            let cursor = &mut &buf[..];
            while !cursor.is_empty() {
                let offset = buf.len() - cursor.len();
                let entry = Entry::decode(cursor);
                yield (current_log_file_id, offset, entry);
            }
        }
    }

    pub fn frozen_file_count(&self) -> usize {
        self.core.frozen_files.read().len()
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
            path: tempdir.path().to_str().unwrap().to_string(),
            // Estimated size of each compressed entry is 111.
            log_file_capacity: 100,
            metrics: Arc::new(RaftLogStoreMetrics::new(0)),
        };
        let log = Log::open(options.clone()).await.unwrap();
        let entries = generate_entries(4, 16, vec![b'x'; 64]);
        assert_eq!(entries.len(), 4);

        for entry in entries.iter().cloned() {
            log.append(vec![entry]).await.unwrap();
        }
        assert_eq!(log.frozen_file_count(), 4);
        let mut buf = vec![];
        for i in 0..4 {
            log.core.frozen_files.write()[i]
                .read_to_end(&mut buf)
                .await
                .unwrap();
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
        assert_eq!(log.frozen_file_count(), 5);
        let mut buf = vec![];
        for i in 0..4 {
            log.core.frozen_files.write()[i]
                .read_to_end(&mut buf)
                .await
                .unwrap();
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
