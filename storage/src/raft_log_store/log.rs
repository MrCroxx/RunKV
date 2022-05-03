use std::path::Path;
use std::sync::Arc;

use futures_async_stream::try_stream;
use itertools::Itertools;
use tokio::fs::{create_dir_all, read_dir, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::trace;

use super::entry::Entry;
use super::error::RaftLogStoreError;
use super::DEFAULT_LOG_BATCH_SIZE;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct LogOptions {
    pub path: String,
    pub log_file_capacity: usize,
}

struct LogCore {
    active_file: File,
    frozen_files: Vec<File>,
    first_log_file_id: u64,
}

pub struct Log {
    path: String,
    log_file_capacity: usize,
    core: Mutex<LogCore>,
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
        let active_file = Self::new_active_file(&options.path, active_file_id).await?;

        let core = LogCore {
            active_file,
            frozen_files,
            first_log_file_id,
        };

        Ok(Self {
            core: Mutex::new(core),
            path: options.path,
            log_file_capacity: options.log_file_capacity,
        })
    }

    pub async fn close(&self) -> Result<()> {
        let guard = self.core.lock().await;
        guard.active_file.sync_all().await?;
        Ok(())
    }

    /// Append [`entry`] to log file. Returns `(file id, offset, len)`.
    pub async fn append(&self, entry: Entry) -> Result<(u64, usize, usize)> {
        let mut guard = self.core.lock().await;
        let file_id = guard.first_log_file_id + guard.frozen_files.len() as u64;
        let start = guard.active_file.metadata().await?.len() as usize;
        let mut buf = Vec::with_capacity(DEFAULT_LOG_BATCH_SIZE);
        entry.encode(&mut buf);
        guard.active_file.write_all(&buf).await?;
        guard.active_file.sync_data().await?;
        let end = guard.active_file.metadata().await?.len() as usize;
        if end >= self.log_file_capacity {
            drop(guard);
            self.rotate().await?;
        }
        Ok((file_id, start, end - start))
    }

    pub async fn read(&self, log_file_id: u64, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut guard = self.core.lock().await;
        let log_file_index = (log_file_id - guard.first_log_file_id) as usize;
        let file = if log_file_index < guard.frozen_files.len() {
            &mut guard.frozen_files[log_file_index]
        } else {
            &mut guard.active_file
        };
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut buf = vec![0; len];
        file.read_exact(&mut buf).await?;
        Ok(buf)
    }

    /// Yield [`(file id, offset, Entry)`] of all frozen logs in order.
    // TODO: Remove clippy exception. Currently clippy reports `needless_lifetime` with
    // `try_stream`.
    #[allow(clippy::needless_lifetimes)]
    #[try_stream(ok = (u64,usize, Entry), error = RaftLogStoreError)]
    pub async fn replay(&self) {
        let guard = self.core.lock().await;
        let begin_log_file_id = guard.first_log_file_id;
        let end_log_file_id = guard.first_log_file_id + guard.frozen_files.len() as u64;
        drop(guard);

        let mut buf = Vec::with_capacity(DEFAULT_LOG_BATCH_SIZE);
        for (i, current_log_file_id) in (begin_log_file_id..end_log_file_id).enumerate() {
            trace!("replay index: {} file id: {}", i, current_log_file_id);
            let mut guard = self.core.lock().await;
            guard.frozen_files[i]
                .seek(std::io::SeekFrom::Start(0))
                .await?;
            buf.clear();
            guard.frozen_files[i].read_to_end(&mut buf).await?;
            drop(guard);
            let cursor = &mut &buf[..];
            while !cursor.is_empty() {
                let offset = buf.len() - cursor.len();
                let entry = Entry::decode(cursor);
                yield (current_log_file_id, offset, entry);
            }
        }
    }

    pub async fn frozen_file_count(&self) -> usize {
        self.core.lock().await.frozen_files.len()
    }
}

impl Log {
    async fn rotate(&self) -> Result<()> {
        let mut guard = self.core.lock().await;
        // Sync old active file.
        guard.active_file.sync_all().await?;
        // Rotate active file.
        let active_file_id = guard.first_log_file_id + guard.frozen_files.len() as u64;
        let new_active_file_id = active_file_id + 1;
        guard.active_file = Self::new_active_file(&self.path, new_active_file_id).await?;
        self.sync_dir().await?;
        trace!(
            "rotate log from {} to {}",
            Self::filename(active_file_id),
            Self::filename(new_active_file_id)
        );
        // Add old active file to frozen file list.
        let frozen_file =
            File::open(Path::new(&self.path).join(Self::filename(active_file_id))).await?;
        guard.frozen_files.push(frozen_file);
        Ok(())
    }

    fn filename(id: u64) -> String {
        format!("{:08}", id)
    }

    async fn sync_dir(&self) -> Result<()> {
        File::open(&self.path).await?.sync_all().await?;
        Ok(())
    }

    async fn new_active_file(path: impl AsRef<Path>, active_file_id: u64) -> Result<File> {
        let mut active_file_open_options = OpenOptions::new();
        active_file_open_options.create(true);
        active_file_open_options.read(true);
        active_file_open_options.append(true);
        let file = active_file_open_options
            .open(path.as_ref().join(Self::filename(active_file_id)))
            .await?;
        Ok(file)
    }
}

pub type LogRef = Arc<Log>;

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::*;
    use crate::raft_log_store::entry::RaftLogBatchBuilder;

    #[test(tokio::test)]
    async fn test_pipe_log_recovery() {
        let tempdir = tempfile::tempdir().unwrap();
        let options = LogOptions {
            path: tempdir.path().to_str().unwrap().to_string(),
            // Estimated size of each compressed entry is 111.
            log_file_capacity: 100,
        };
        let log = Log::open(options.clone()).await.unwrap();
        let entries = generate_entries(4, 16, vec![b'x'; 64]);
        assert_eq!(entries.len(), 4);

        for entry in entries.iter().cloned() {
            log.append(entry).await.unwrap();
        }
        assert_eq!(log.core.lock().await.frozen_files.len(), 4);
        let mut buf = vec![];
        for i in 0..4 {
            log.core.lock().await.frozen_files[i]
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
        assert_eq!(log.core.lock().await.frozen_files.len(), 5);
        let mut buf = vec![];
        for i in 0..4 {
            log.core.lock().await.frozen_files[i]
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
