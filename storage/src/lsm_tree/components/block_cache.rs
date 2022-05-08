use std::sync::Arc;
use std::time::Instant;

use bytes::BufMut;
use futures::Future;
use lazy_static::lazy_static;
use moka::future::Cache;
use prometheus;

use super::Block;
use crate::lsm_tree::DEFAULT_BLOCK_SIZE;
use crate::{Error, Result};

lazy_static! {
    static ref BLOCK_CACHE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "block_cache_latency_histogram_vec",
            "block_cache latency histogram vec",
            &["op", "node"]
        )
        .unwrap();
}

struct BlockCacheMetrics {
    block_cache_get_latency_histogram: prometheus::Histogram,
    block_cache_insert_latency_histogram: prometheus::Histogram,
    block_cache_fill_latency_histogram: prometheus::Histogram,
}
impl BlockCacheMetrics {
    fn new(node: u64) -> Self {
        Self {
            block_cache_get_latency_histogram: BLOCK_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_get", &node.to_string()])
                .unwrap(),
            block_cache_insert_latency_histogram: BLOCK_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_insert", &node.to_string()])
                .unwrap(),
            block_cache_fill_latency_histogram: BLOCK_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_fill", &node.to_string()])
                .unwrap(),
        }
    }
}
pub struct BlockCache {
    inner: Cache<Vec<u8>, Arc<Block>>,
    metrics: BlockCacheMetrics,
}

impl BlockCache {
    pub fn new(capacity: usize, node: u64) -> Self {
        let cache: Cache<Vec<u8>, Arc<Block>> = Cache::builder()
            .weigher(|_k, v: &Arc<Block>| v.len() as u32)
            .initial_capacity(capacity / DEFAULT_BLOCK_SIZE)
            .max_capacity(capacity as u64)
            .build();

        Self {
            inner: cache,
            metrics: BlockCacheMetrics::new(node),
        }
    }

    pub fn get(&self, sst_id: u64, block_idx: usize) -> Option<Arc<Block>> {
        let start = Instant::now();

        let result = self.inner.get(&Self::key(sst_id, block_idx));

        self.metrics
            .block_cache_get_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        result
    }

    pub async fn insert(&self, sst_id: u64, block_idx: usize, block: Arc<Block>) {
        let start = Instant::now();

        self.inner.insert(Self::key(sst_id, block_idx), block).await;

        self.metrics
            .block_cache_insert_latency_histogram
            .observe(start.elapsed().as_secs_f64());
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
            .get_or_try_insert_with(Self::key(sst_id, block_idx), future)
            .await
        {
            Ok(block) => Ok(block),
            Err(arc_error) => Err(Error::Other(arc_error.to_string())),
        };

        self.metrics
            .block_cache_get_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        result
    }

    fn key(sst_id: u64, block_idx: usize) -> Vec<u8> {
        let mut key = Vec::with_capacity(16);
        key.put_u64_le(sst_id);
        key.put_u64_le(block_idx as u64);
        key
    }
}
