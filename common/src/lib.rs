pub mod atomic;
pub mod channel_pool;
pub mod coding;
pub mod config;
pub mod context;
pub mod log;
pub mod notify_pool;
pub mod packer;
pub mod prometheus;
pub mod sharded_hash_map;
pub mod sync;
pub mod time;
pub mod tracing_slog_drain;

use async_trait::async_trait;

#[async_trait]
pub trait Worker: Sync + Send + 'static {
    async fn run(&mut self) -> anyhow::Result<()>;
}

pub type BoxedWorker = Box<dyn Worker>;
