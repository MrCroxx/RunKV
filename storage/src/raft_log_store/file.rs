use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{trace_span, Instrument};

use crate::error::Result;

fn filename(id: u64) -> String {
    format!("{:08}", id)
}

pub async fn sync_dir(dir: impl AsRef<Path>) -> Result<()> {
    fs::File::open(dir.as_ref())
        .await?
        .sync_all()
        .instrument(trace_span!("sync_all"))
        .await?;
    Ok(())
}

struct ActiveFileCore {
    file: fs::File,
    offset: usize,
}

#[derive(Clone)]
pub struct ActiveFile {
    id: u64,
    core: Arc<Mutex<ActiveFileCore>>,
}

impl std::fmt::Debug for ActiveFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveFile").field("id", &self.id).finish()
    }
}

impl ActiveFile {
    #[tracing::instrument(level = "trace", skip(dir), err)]
    pub async fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.append(true);
        let file = options.open(dir.as_ref().join(filename(id))).await?;

        Ok(Self {
            id,
            core: Arc::new(Mutex::new(ActiveFileCore { file, offset: 0 })),
        })
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[tracing::instrument(level = "trace", ret)]
    pub async fn offset(&self) -> u64 {
        self.core.lock().await.offset as u64
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn append(&self, buf: &[u8]) -> Result<(u64, usize)> {
        let mut core = self.core.lock().await;
        let offset = core.offset as u64;
        let len = buf.len();
        core.file
            .write_all(buf)
            .instrument(trace_span!("write_all"))
            .await?;
        core.offset += len;
        Ok((offset, len))
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut core = self.core.lock().await;
        core.file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0; len];
        core.file.read_exact(&mut buf).await?;
        Ok(buf)
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn flush(&self) -> Result<()> {
        self.core.lock().await.file.flush().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn sync_data(&self) -> Result<()> {
        self.core
            .lock()
            .await
            .file
            .sync_data()
            .instrument(trace_span!("sync_data"))
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn sync_all(&self) -> Result<()> {
        self.core
            .lock()
            .await
            .file
            .sync_all()
            .instrument(trace_span!("sync_all"))
            .await?;
        Ok(())
    }
}

struct FrozenFileCore {
    file: fs::File,
}

#[derive(Clone)]
pub struct FrozenFile {
    id: u64,
    core: Arc<Mutex<FrozenFileCore>>,
}

impl std::fmt::Debug for FrozenFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenFile").field("id", &self.id).finish()
    }
}

impl FrozenFile {
    #[tracing::instrument(level = "trace", skip(dir), err)]
    pub async fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let mut options = fs::OpenOptions::new();
        options.read(true);
        let file = options.open(dir.as_ref().join(filename(id))).await?;

        Ok(Self {
            id,
            core: Arc::new(Mutex::new(FrozenFileCore { file })),
        })
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut core = self.core.lock().await;
        core.file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0; len];
        core.file.read_exact(&mut buf).await?;
        Ok(buf)
    }

    #[tracing::instrument(level = "trace", err)]
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        let mut core = self.core.lock().await;
        core.file.seek(SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        core.file.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}
