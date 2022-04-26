pub mod atomic;
pub mod channel_pool;
pub mod coding;
pub mod config;
pub mod log;
pub mod notify_pool;
pub mod time;

use async_trait::async_trait;

#[async_trait]
pub trait Worker: Sync + Send + 'static {
    async fn run(&mut self) -> anyhow::Result<()>;
}

pub type BoxedWorker = Box<dyn Worker>;
