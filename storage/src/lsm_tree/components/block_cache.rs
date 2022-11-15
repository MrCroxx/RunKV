use core::ops::Deref;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use futures::Future;

use super::metrics::LsmTreeMetricsRef;
use super::Block;
use crate::tiered_cache::{TieredCacheEntry, TieredCacheValue};
use crate::utils::lru_cache::{CacheableEntry, LruCache, LruCacheEventListener};
use crate::{Error, Result};

const SHARD_BITS: usize = 6;

enum BlockEntry {
    Cache(CacheableEntry<(u64, usize), Box<Block>>),
    Owned(Box<Block>),
    RefEntry(Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    block: *const Block,
}

impl BlockHolder {
    pub fn from_ref_block(block: Arc<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::RefEntry(block),
            block: ptr,
        }
    }

    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_cached_block(entry: CacheableEntry<(u64, usize), Box<Block>>) -> Self {
        let ptr = entry.value().as_ref() as *const _;
        Self {
            _handle: BlockEntry::Cache(entry),
            block: ptr,
        }
    }

    pub fn from_tiered_cache(entry: TieredCacheEntry<(u64, usize), Box<Block>>) -> Self {
        match entry {
            TieredCacheEntry::Cache(entry) => Self::from_cached_block(entry),
            TieredCacheEntry::Owned(block) => Self::from_owned_block(*block),
        }
    }
}

impl Deref for BlockHolder {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

type BlockCacheEventListener = Arc<dyn LruCacheEventListener<K = (u64, usize), T = Box<Block>>>;

pub struct BlockCache {
    inner: Arc<LruCache<(u64, usize), Box<Block>>>,
    metrics: LsmTreeMetricsRef,
}

impl BlockCache {
    pub fn new(capacity: usize, metrics: LsmTreeMetricsRef) -> Self {
        Self::new_inner(capacity, metrics, None)
    }

    pub fn with_event_listener(
        capacity: usize,
        metrics: LsmTreeMetricsRef,
        listener: BlockCacheEventListener,
    ) -> Self {
        Self::new_inner(capacity, metrics, Some(listener))
    }

    fn new_inner(
        capacity: usize,
        metrics: LsmTreeMetricsRef,
        listener: Option<BlockCacheEventListener>,
    ) -> Self {
        let inner = match listener {
            Some(listener) => LruCache::with_event_listener(SHARD_BITS, capacity, listener),
            None => LruCache::new(SHARD_BITS, capacity),
        };
        let inner = Arc::new(inner);

        Self { inner, metrics }
    }

    pub fn get(&self, sst_id: u64, block_idx: usize) -> Option<BlockHolder> {
        let start = Instant::now();

        let result = self
            .inner
            .lookup(Self::hash(sst_id, block_idx), &(sst_id, block_idx))
            .map(BlockHolder::from_cached_block);

        self.metrics
            .block_cache_get_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        result
    }

    pub async fn insert(&self, sst_id: u64, block_idx: usize, block: Box<Block>) -> BlockHolder {
        let start = Instant::now();

        let result = self.inner.insert(
            (sst_id, block_idx),
            Self::hash(sst_id, block_idx),
            block.raw().len(),
            block,
        );
        let result = BlockHolder::from_cached_block(result);

        self.metrics
            .block_cache_insert_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        result
    }

    pub async fn get_or_insert_with<F, Fut>(
        &self,
        sst_id: u64,
        block_idx: usize,
        mut fetch: F,
    ) -> Result<BlockHolder>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Box<Block>>> + Send + 'static,
    {
        let hash = Self::hash(sst_id, block_idx);
        let key = (sst_id, block_idx);

        let start = Instant::now();

        let result = self
            .inner
            .lookup_with_request_dedup(hash, key, || {
                let f = fetch();

                async move {
                    let block = f.await?;
                    let len = block.encoded_len();
                    Ok((block, len))
                }
            })
            .await
            .map_err(Error::err)?
            .map(BlockHolder::from_cached_block);

        self.metrics
            .block_cache_get_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        result
    }

    fn hash(sst_id: u64, block_idx: usize) -> u64 {
        let mut hasher = DefaultHasher::default();
        sst_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        hasher.finish()
    }
}
