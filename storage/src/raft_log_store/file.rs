use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::error::Result;

pub async fn sync_dir(dir: impl AsRef<Path>) -> Result<()> {
    let path = dir.as_ref().to_path_buf();
    let dir = tokio::fs::File::open(path).await?;
    dir.sync_all().await?;
    Ok(())
}

fn filename(id: u64) -> String {
    format!("{:08}", id)
}

#[derive(Clone)]
pub struct ActiveFile {
    id: u64,
    file: Arc<tokio::sync::Mutex<tokio::fs::File>>,
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
        let path = dir.as_ref().join(filename(id));
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true);
        options.append(true);
        options.read(true);
        let file = options.open(path).await?;

        Ok(Self {
            id,
            file: Arc::new(tokio::sync::Mutex::new(file)),
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
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = self.file.lock().await;

        let mut buf = vec![0; len];
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.read_exact(&mut buf).await?;

        Ok(buf)
    }

    // TODO: Replace `buf: Vec<u8>` with `buf: Bytes` or `buf: &[u8]` (but 'static is needed).
    #[tracing::instrument(level = "trace", err)]
    pub async fn append(&self, buf: Vec<u8>) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut file = self.file.lock().await;
        file.write_all(&buf).await?;
        self.len.fetch_add(buf.len(), Ordering::Release);

        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn flush(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        self.file.lock().await.flush().await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn sync_data(&self) -> Result<()> {
        self.file.lock().await.sync_data().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn sync_all(&self) -> Result<()> {
        self.file.lock().await.sync_all().await?;
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
