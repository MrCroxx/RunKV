mod mem;
pub use mem::*;
mod s3;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
pub use s3::*;

use crate::Result;

#[derive(thiserror::Error, Debug)]
pub enum ObjectStoreError {
    #[error("object not found: {0}")]
    ObjectNotFound(String),
    #[error("invalid range: {0}")]
    InvalidRange(String),
    #[error("S3 error: {0}")]
    S3(String),
    #[error("other: {0}")]
    Other(String),
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, path: &str, obj: Vec<u8>) -> Result<()>;

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>>;

    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Option<Vec<u8>>>;

    async fn remove(&self, path: &str) -> Result<()>;
}

pub type ObjectStoreRef = Arc<dyn ObjectStore>;
