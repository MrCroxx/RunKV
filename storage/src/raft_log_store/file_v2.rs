use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::error::Result;

pub async fn sync_dir(dir: impl AsRef<Path>) -> Result<()> {
    let path = dir.as_ref().to_path_buf();

    let (tx, rx) = oneshot::channel();
    tokio::task::spawn_blocking(move || {
        let dir = match std::fs::File::open(path) {
            Ok(dir) => dir,
            Err(e) => {
                tx.send(Err(e)).unwrap();
                return;
            }
        };
        tx.send(dir.sync_all()).unwrap();
    });

    rx.await.unwrap()?;

    Ok(())
}

fn filename(id: u64) -> String {
    format!("{:08}", id)
}

#[derive(Clone)]
pub struct ActiveFile {
    id: u64,
    file: Arc<std::fs::File>,
    len: Arc<AtomicUsize>,
}

impl std::fmt::Debug for ActiveFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveFile").field("id", &self.id).finish()
    }
}

impl ActiveFile {
    #[tracing::instrument(level = "trace", skip(dir), err)]
    pub async fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        let path = dir.as_ref().join(filename(id));

        tokio::task::spawn_blocking(move || {
            let mut options = std::fs::OpenOptions::new();
            options.create(true);
            options.append(true);
            options.read(true);
            let result = options.open(path);
            tx.send(result).unwrap();
        });

        let file = rx.await.unwrap()?;

        Ok(Self {
            id,
            file: Arc::new(file),
            len: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let f = self.file.clone();

        tokio::task::spawn_blocking(move || {
            use std::os::unix::prelude::FileExt;
            let mut buf = vec![0; len];
            let result = f.read_exact_at(&mut buf, offset).map(|_| buf);
            tx.send(result).unwrap();
        });

        let buf = rx.await.unwrap()?;

        Ok(buf)
    }

    // TODO: Replace `buf: Vec<u8>` with `buf: Bytes` or `buf: &[u8]` (but 'static is needed).
    #[tracing::instrument(level = "trace", err)]
    pub async fn append(&self, buf: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let f = self.file.clone();
        let buf_len = buf.len();

        tokio::task::spawn_blocking(move || {
            use std::os::unix::prelude::FileExt;
            let result = f.write_all_at(&buf, 0);
            tx.send(result).unwrap();
        });

        rx.await.unwrap()?;
        self.len.fetch_add(buf_len, Ordering::Release);

        Ok(())
    }

    /// # Safety
    /// Concurrent `flush` call leads to panics.
    #[tracing::instrument(level = "trace", err)]
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let mut f = self.file.clone();

        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let fmut = Arc::get_mut(&mut f).unwrap();
            let result = fmut.flush();
            tx.send(result).unwrap();
        });

        rx.await.unwrap()?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn sync_data(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let f = self.file.clone();

        tokio::task::spawn_blocking(move || {
            let result = f.sync_data();
            tx.send(result).unwrap();
        });

        rx.await.unwrap()?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn sync_all(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let f = self.file.clone();

        tokio::task::spawn_blocking(move || {
            let result = f.sync_all();
            tx.send(result).unwrap();
        });

        rx.await.unwrap()?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct FrozenFile {
    id: u64,
    file: Arc<std::fs::File>,
}

impl std::fmt::Debug for FrozenFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenFile").field("id", &self.id).finish()
    }
}

impl FrozenFile {
    #[tracing::instrument(level = "trace", skip(dir), err)]
    pub async fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        let path = dir.as_ref().join(filename(id));

        tokio::task::spawn_blocking(move || {
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            let result = options.open(path);
            tx.send(result).unwrap();
        });

        let file = rx.await.unwrap()?;

        Ok(Self {
            id,
            file: Arc::new(file),
        })
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let f = self.file.clone();

        tokio::task::spawn_blocking(move || {
            use std::os::unix::prelude::FileExt;
            let mut buf = vec![0; len];
            let result = f.read_exact_at(&mut buf, offset).map(|_| buf);
            tx.send(result).unwrap();
        });

        let buf = rx.await.unwrap()?;

        Ok(buf)
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let f = self.file.clone();

        tokio::task::spawn_blocking(move || {
            use std::os::unix::prelude::FileExt;
            let len = match f.metadata() {
                Ok(metadata) => metadata.len() as usize,
                Err(e) => {
                    tx.send(Err(e)).unwrap();
                    return;
                }
            };
            let mut buf = vec![0; len];
            let result = f.read_exact_at(&mut buf, 0).map(|_| buf);
            tx.send(result).unwrap();
        });

        let buf = rx.await.unwrap()?;

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<ActiveFile>();
        is_send_sync_clone::<FrozenFile>();
    }
}
