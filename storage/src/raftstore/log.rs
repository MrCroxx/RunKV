use std::path::Path;

use itertools::Itertools;
use tokio::fs::{create_dir_all, read_dir, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::trace;

use crate::entry::Entry;
use crate::error::{Error, Result};

const DEFAULT_LOG_BATCH_SIZE: usize = 8 << 10;

#[derive(Clone, Debug)]
pub struct PipeLogOptions {
    path: String,
    log_file_capacity: usize,
}

struct PipeLogCore {
    active_file: File,
    frozen_files: Vec<File>,
    first_log_file_id: u64,
}

pub struct PipeLog {
    path: String,
    log_file_capacity: usize,
    core: Mutex<PipeLogCore>,
}

impl PipeLog {
    pub async fn open(options: PipeLogOptions) -> Result<Self> {
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
                        .map_err(|s| Error::Other(format!("invalid file name: {:?}", s)))?
                        .parse::<u64>()
                        .map_err(|e| Error::Other(format!("invalid file name: {}", e)))?,
                    file,
                ));
            }
            if frozen_files.is_empty() {
                (vec![], 0)
            } else {
                frozen_files.sort_by_key(|(id, _)| *id);
                let first_log_file_id = frozen_files[0].0;
                for (i, frozen_file) in frozen_files.iter().enumerate() {
                    if frozen_file.0 != first_log_file_id + i as u64 {
                        return Err(Error::Other(format!(
                            "log file {} is missing",
                            first_log_file_id + i as u64
                        )));
                    }
                }
                (
                    frozen_files.into_iter().map(|(_, file)| file).collect_vec(),
                    first_log_file_id,
                )
            }
        };
        let active_file_id = first_log_file_id + frozen_files.len() as u64 + 1;
        let active_file = Self::new_active_file(&options.path, active_file_id).await?;

        let core = PipeLogCore {
            active_file,
            frozen_files,
            first_log_file_id,
        };

        // TODO: Replay frozen logs.

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

    pub async fn append(&self, entries: Vec<Entry>) -> Result<()> {
        let mut guard = self.core.lock().await;
        let mut buf = Vec::with_capacity(DEFAULT_LOG_BATCH_SIZE);
        for entry in entries {
            entry.encode(&mut buf);
        }
        guard.active_file.write_all(&buf).await?;
        guard.active_file.sync_data().await?;
        if guard.active_file.metadata().await?.len() as usize >= self.log_file_capacity {
            drop(guard);
            self.rotate().await?;
        }
        Ok(())
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
}

impl PipeLog {
    async fn rotate(&self) -> Result<()> {
        let mut guard = self.core.lock().await;
        // Sync old active file.
        guard.active_file.sync_all().await?;
        // Rotate active file.
        let active_file_id = guard.first_log_file_id + guard.frozen_files.len() as u64 + 1;
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

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::*;
    use crate::entry::RaftLog;

    #[test(tokio::test)]
    async fn test_pipe_log_recovery() {
        let tempdir = tempfile::tempdir().unwrap();
        let options = PipeLogOptions {
            path: tempdir.path().to_str().unwrap().to_string(),
            log_file_capacity: 1024,
        };
        let log = PipeLog::open(options.clone()).await.unwrap();
        let batches = generate_log_batches(4, 16, vec![b'x'; 64]);
        for entries in batches.clone() {
            log.append(entries).await.unwrap();
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
        let decoded_batches = (0..4)
            .into_iter()
            .map(|_| {
                (0..16)
                    .into_iter()
                    .map(|_| Entry::decode(&mut buf))
                    .collect_vec()
            })
            .collect_vec();
        assert_eq!(decoded_batches, batches);
        log.close().await.unwrap();

        // Recover pipe log.
        drop(log);
        let log = PipeLog::open(options).await.unwrap();
        assert_eq!(log.core.lock().await.frozen_files.len(), 5);
        let mut buf = vec![];
        for i in 0..4 {
            log.core.lock().await.frozen_files[i]
                .read_to_end(&mut buf)
                .await
                .unwrap();
        }
        let mut buf = &buf[..];
        let decoded_batches = (0..4)
            .into_iter()
            .map(|_| {
                (0..16)
                    .into_iter()
                    .map(|_| Entry::decode(&mut buf))
                    .collect_vec()
            })
            .collect_vec();
        assert_eq!(decoded_batches, batches);
    }

    fn generate_log_batches(
        batch_count: usize,
        batch_size: usize,
        data: Vec<u8>,
    ) -> Vec<Vec<Entry>> {
        let mut batches = vec![];
        let mut index = 0;
        for _ in 0..batch_count {
            let mut batch = vec![];
            for _ in 0..batch_size {
                index += 1;
                batch.push(Entry::RaftLog(RaftLog::Entry {
                    group: 1,
                    term: 1,
                    index,
                    data: data.clone(),
                }));
            }
            batches.push(batch);
        }
        batches
    }
}
