use std::sync::Arc;
use std::time::Instant;

use futures::Future;
use moka::future::Cache;

use super::error::RaftLogStoreError;
use super::metrics::RaftLogStoreMetricsRef;
use super::DEFAULT_LOG_BATCH_SIZE;
use crate::error::Result;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
struct BlockIndex {
    file_id: u64,
    offset: usize,
}

pub struct BlockCache {
    inner: Cache<BlockIndex, Arc<Vec<u8>>>,
    metrics: RaftLogStoreMetricsRef,
}

impl BlockCache {
    pub fn new(capacity: usize, metrics: RaftLogStoreMetricsRef) -> Self {
        let cache: Cache<BlockIndex, Arc<Vec<u8>>> = Cache::builder()
            .weigher(|_k, v: &Arc<Vec<u8>>| v.len() as u32)
            .initial_capacity(capacity / DEFAULT_LOG_BATCH_SIZE)
            .max_capacity(capacity as u64)
            .build();
        Self {
            inner: cache,
            metrics,
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get(&self, file_id: u64, offset: usize) -> Option<Arc<Vec<u8>>> {
        let start = Instant::now();

        let result = self.inner.get(&BlockIndex { file_id, offset });

        self.metrics
            .block_cache_get_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        result
    }

    #[tracing::instrument(level = "trace", skip(self, block))]
    pub async fn insert(&self, file_id: u64, offset: usize, block: Arc<Vec<u8>>) {
        let start = Instant::now();

        self.inner
            .insert(BlockIndex { file_id, offset }, block)
            .await;

        self.metrics
            .block_cache_insert_latency_histogram
            .observe(start.elapsed().as_secs_f64());
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
        let future = async move {
            let start_fill = Instant::now();

            let r = f.await;

            self.metrics
                .block_cache_fill_latency_histogram
                .observe(start_fill.elapsed().as_secs_f64());

            r
        };

        let start = Instant::now();

        let result = match self
            .inner
            .get_or_try_insert_with(BlockIndex { file_id, offset }, future)
            .await
        {
            Ok(block) => block,
            Err(arc_error) => return Err(RaftLogStoreError::Other(arc_error.to_string()).into()),
        };

        self.metrics
            .block_cache_get_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        Ok(result)
    }
}
