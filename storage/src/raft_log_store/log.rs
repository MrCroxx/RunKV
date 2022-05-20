use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use futures_async_stream::try_stream;
use itertools::Itertools;
use runkv_common::packer::{Item, Packer};
use tokio::fs::{create_dir_all, read_dir};
use tokio::sync::{oneshot, Mutex};
use tracing::trace;

use super::entry::Entry;
use super::error::RaftLogStoreError;
use super::file::{ActiveFile, FrozenFile};
use super::metrics::RaftLogStoreMetricsRef;
use super::queue::{LogFile, LogQueue};
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

#[derive(Clone)]
pub struct Log {
    node: u64,
    path: String,
    log_file_capacity: usize,
    persist: Persist,

    queue: LogQueue,
    packer: Packer<Vec<Entry>, Vec<WriteHandle>>,

    mutex: Arc<Mutex<PhantomData<()>>>,

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

        let queue = LogQueue::init(options.node, active_file, frozen_files)?;
        let packer = Packer::new(64);

        Ok(Self {
            node: options.node,
            path: options.path,
            log_file_capacity: options.log_file_capacity,
            persist: options.persist,

            queue,
            packer,

            mutex: Arc::new(Mutex::new(PhantomData::default())),

            metrics: options.metrics,
        })
    }

    #[tracing::instrument(level = "trace", ret)]
    pub async fn frozen_file_count(&self) -> usize {
        self.queue.frozen_file_count()
    }

    /// Append [`entries`] to log file.
    #[tracing::instrument(level = "trace", ret, err)]
    pub async fn append(&self, entries: Vec<Entry>) -> Result<Vec<WriteHandle>> {
        let start = Instant::now();
        let mut total_size = 0;

        let (tx, rx) = oneshot::channel();
        // Append entries to queue.
        let is_leader = self.packer.append(entries, Some(tx));

        if is_leader {
            // Get exclusive lock to prevent next batch write concurrently.
            let guard = self.mutex.lock().await;

            let mut active_file = self.queue.active();
            let mut offset = active_file.len();

            let mut buffer = Vec::with_capacity(DEFAULT_BUFFER_SIZE);

            // Take writer batch.
            let writers = self.packer.package();
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

                let file_id = active_file.id();

                for entry in entries {
                    let entry_offset = offset + buffer.len();
                    entry.encode(&mut buffer);
                    let entry_len = offset + buffer.len() - entry_offset;
                    total_size += entry_len;
                    entry_handles.push(WriteHandle {
                        file_id,
                        offset: entry_offset,
                        len: entry_len,
                    });
                }

                if offset + buffer.len() >= self.log_file_capacity {
                    // Force write buffer.
                    active_file.append(&buffer).await?;
                    self.metrics
                        .sync_size_histogram
                        .observe(buffer.len() as f64);
                    buffer.clear();

                    // Sync old active file.
                    let now = Instant::now();
                    active_file.sync_all().await?;
                    self.metrics
                        .sync_latency_histogram
                        .observe(now.elapsed().as_secs_f64());

                    // Rotate log queue.
                    trace!(
                        "rotate log from {} to {}",
                        filename(file_id),
                        filename(file_id + 1)
                    );
                    let new_active_file = ActiveFile::open(&self.path, file_id + 1).await?;
                    let new_frozen_file = FrozenFile::open(&self.path, file_id).await?;
                    active_file = new_active_file.clone();
                    self.queue.rotate(new_active_file, new_frozen_file).await;
                    offset = 0;

                    // Sync dir.
                    let start_sync = Instant::now();
                    sync_dir(&self.path).await?;
                    self.metrics
                        .sync_latency_histogram
                        .observe(start_sync.elapsed().as_secs_f64());
                }

                txs.push(notifier.unwrap());
                handles.push(entry_handles);
            }

            if !buffer.is_empty() {
                active_file.append(&buffer).await?;
                self.metrics
                    .sync_size_histogram
                    .observe(buffer.len() as f64);
                buffer.clear();

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

            drop(guard);
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
        let file = self.queue.file(log_file_id)?;
        let buf = match file {
            LogFile::Active(file) => file.read(block_offset, block_len).await?,
            LogFile::Frozen(file) => file.read(block_offset, block_len).await?,
        };
        Ok(buf)
    }

    /// Yield [`(file id, offset, Entry)`] of all frozen logs in order.
    // TODO: Remove clippy exception. Currently clippy reports `needless_lifetime` with
    // `try_stream`.
    #[allow(clippy::needless_lifetimes)]
    #[try_stream(ok = (u64,usize, Entry), error = Error)]
    pub async fn replay(&self) {
        let frozen_files = self.queue.frozens();

        for frozen_file in frozen_files {
            trace!("replay file: {:?}", frozen_file);
            let log_file_id = frozen_file.id();
            let buf = frozen_file.read_all().await?;
            let cursor = &mut &buf[..];
            while !cursor.is_empty() {
                let offset = buf.len() - cursor.len();
                let entry = Entry::decode(cursor);
                yield (log_file_id, offset, entry);
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.queue.active().sync_all().await?;
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
            buf.append(&mut log.queue.frozens()[i].read_all().await.unwrap());
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
            buf.append(&mut log.queue.frozens()[i].read_all().await.unwrap());
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
