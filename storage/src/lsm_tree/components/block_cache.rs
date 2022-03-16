use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::Future;
use moka::future::Cache;

use super::Block;
use crate::lsm_tree::DEFAULT_BLOCK_SIZE;
use crate::{Error, Result};

pub struct BlockCache {
    inner: Cache<Bytes, Arc<Block>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cache: Cache<Bytes, Arc<Block>> = Cache::builder()
            .weigher(|_k, v: &Arc<Block>| v.len() as u32)
            .initial_capacity(capacity / DEFAULT_BLOCK_SIZE)
            .max_capacity(capacity as u64)
            .build();
        Self { inner: cache }
    }

    pub fn get(&self, sst_id: u64, block_idx: usize) -> Option<Arc<Block>> {
        self.inner.get(&Self::key(sst_id, block_idx))
    }

    pub async fn insert(&self, sst_id: u64, block_idx: usize, block: Arc<Block>) {
        self.inner.insert(Self::key(sst_id, block_idx), block).await
    }

    pub async fn get_or_insert_with<F>(
        &self,
        sst_id: u64,
        block_idx: usize,
        f: F,
    ) -> Result<Arc<Block>>
    where
        F: Future<Output = Result<Arc<Block>>>,
    {
        match self
            .inner
            .get_or_try_insert_with(Self::key(sst_id, block_idx), f)
            .await
        {
            Ok(block) => Ok(block),
            Err(arc_error) => Err(Error::Other(arc_error.to_string())),
        }
    }

    fn key(sst_id: u64, block_idx: usize) -> Bytes {
        let mut key = BytesMut::with_capacity(16);
        key.put_u64_le(sst_id);
        key.put_u64_le(block_idx as u64);
        key.freeze()
    }
}
