use std::sync::Arc;

use futures::Future;
use moka::future::Cache;
use tracing::trace;

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

    pub fn get(&self, file_id: u64, offset: usize) -> Option<Arc<Vec<u8>>> {
        trace!(
            file_id = file_id,
            offset = offset,
            "try get from block cache:"
        );
        self.inner.get(&BlockIndex { file_id, offset })
    }

    pub async fn insert(&self, file_id: u64, offset: usize, block: Arc<Vec<u8>>) {
        trace!(
            file_id = file_id,
            offset = offset,
            len = block.len(),
            "insert to block cache:"
        );
        self.inner
            .insert(BlockIndex { file_id, offset }, block)
            .await
    }

    pub async fn get_or_insert_with<F>(
        &self,
        file_id: u64,
        offset: usize,
        f: F,
    ) -> Result<Arc<Vec<u8>>>
    where
        F: Future<Output = Result<Arc<Vec<u8>>>>,
    {
        trace!(
            file_id = file_id,
            offset = offset,
            "get or insert block cache"
        );
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
