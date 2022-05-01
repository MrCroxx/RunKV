use std::sync::Arc;

use futures::Future;
use moka::future::Cache;

use super::error::RaftLogStoreError;
use super::DEFAULT_LOG_BATCH_SIZE;
use crate::error::Result;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
struct BlockIndex {
    file_id: u64,
    offset: usize,
}

pub struct BlockCache {
    inner: Cache<BlockIndex, Arc<Vec<u8>>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cache: Cache<BlockIndex, Arc<Vec<u8>>> = Cache::builder()
            .weigher(|_k, v: &Arc<Vec<u8>>| v.len() as u32)
            .initial_capacity(capacity / DEFAULT_LOG_BATCH_SIZE)
            .max_capacity(capacity as u64)
            .build();
        Self { inner: cache }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get(&self, file_id: u64, offset: usize) -> Option<Arc<Vec<u8>>> {
        self.inner.get(&BlockIndex { file_id, offset })
    }

    #[tracing::instrument(level = "trace", skip(self, block))]
    pub async fn insert(&self, file_id: u64, offset: usize, block: Arc<Vec<u8>>) {
        self.inner
            .insert(BlockIndex { file_id, offset }, block)
            .await
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub async fn get_or_insert_with<F>(
        &self,
        file_id: u64,
        offset: usize,
        f: F,
    ) -> Result<Arc<Vec<u8>>>
    where
        F: Future<Output = Result<Arc<Vec<u8>>>>,
    {
        match self
            .inner
            .get_or_try_insert_with(BlockIndex { file_id, offset }, f)
            .await
        {
            Ok(block) => Ok(block),
            Err(arc_error) => Err(RaftLogStoreError::Other(arc_error.to_string()).into()),
        }
    }
}
