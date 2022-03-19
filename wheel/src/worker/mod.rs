pub mod sstable_uploader;
pub mod version_syncer;

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Worker: Sync + Send + 'static {
    async fn run(&mut self) -> Result<()>;
}

pub type BoxedWorker = Box<dyn Worker>;
