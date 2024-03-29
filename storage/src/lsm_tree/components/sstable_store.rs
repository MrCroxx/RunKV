use std::mem::size_of;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use moka::future::Cache;

use super::{Block, BlockCache, BlockHolder, Sstable, SstableMeta};
use crate::object_store::ObjectStoreRef;
use crate::tiered_cache::{TieredCache, TieredCacheKey, TieredCacheValue};
use crate::utils::lru_cache::LruCacheEventListener;
use crate::{Error, ObjectStoreError, Result};

impl TieredCacheKey for (u64, usize) {
    fn encoded_len() -> usize {
        16
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
        buf.put_u64(self.1 as u64);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let sst_id = buf.get_u64();
        let block_idx = buf.get_u64() as usize;
        (sst_id, block_idx)
    }
}

impl TieredCacheValue for Box<Block> {
    fn len(&self) -> usize {
        self.raw().len()
    }

    fn encoded_len(&self) -> usize {
        self.raw().len()
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_slice(self.raw());
    }

    fn decode(buf: Vec<u8>) -> Self {
        Box::new(Block::from_raw(buf))
    }
}

pub struct BlockCacheEventListener {
    tiered_cache: TieredCache<(u64, usize), Box<Block>>,
}

impl BlockCacheEventListener {
    pub fn new(tiered_cache: TieredCache<(u64, usize), Box<Block>>) -> Self {
        Self { tiered_cache }
    }
}

impl LruCacheEventListener for BlockCacheEventListener {
    type K = (u64, usize);
    type T = Box<Block>;

    fn on_release(&self, key: Self::K, value: Self::T) {
        // TODO(MrCroxx): handle error?
        self.tiered_cache.insert(key, value).unwrap();
    }
}

// TODO: Define policy based on use cases (read / comapction / ...).
#[derive(PartialEq, Clone, Copy)]
pub enum CachePolicy {
    Disable,
    Fill,
    NotFill,
}

pub struct SstableStoreOptions {
    pub path: String,
    pub object_store: ObjectStoreRef,
    pub block_cache: BlockCache,
    pub meta_cache_capacity: usize,
    pub tiered_cache: TieredCache<(u64, usize), Box<Block>>,
}

pub struct SstableStore {
    path: String,
    object_store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Cache<u64, Arc<SstableMeta>>,
    tiered_cache: TieredCache<(u64, usize), Box<Block>>,
}

impl SstableStore {
    pub fn new(options: SstableStoreOptions) -> Self {
        Self {
            path: options.path,
            object_store: options.object_store,
            block_cache: options.block_cache,
            meta_cache: Cache::new(
                (options.meta_cache_capacity / size_of::<SstableMeta>() + 1) as u64,
            ),
            tiered_cache: options.tiered_cache,
        }
    }

    pub async fn put(&self, sst: &Sstable, data: Vec<u8>, policy: CachePolicy) -> Result<()> {
        let data_path = self.data_path(sst.id());
        self.object_store.put(&data_path, data.clone()).await?;

        let meta = sst.encode_meta();
        let meta_path = self.meta_path(sst.id());
        if let Err(e) = self.object_store.put(&meta_path, meta).await {
            self.object_store.remove(&data_path).await?;
            return Err(e);
        }

        if let CachePolicy::Fill = policy {
            for (block_idx, meta) in sst.block_metas_iter().enumerate() {
                let block = Box::new(Block::decode(&data[meta.data_range()])?);
                self.block_cache.insert(sst.id(), block_idx, block).await;
            }
        }

        Ok(())
    }

    pub async fn block(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
    ) -> Result<BlockHolder> {
        let fetch_block = || {
            let data_path = self.data_path(sst.id());
            let object_store = self.object_store.clone();
            let sst = sst.clone();
            let tiered_cache = if policy == CachePolicy::Disable {
                TieredCache::none()
            } else {
                self.tiered_cache.clone()
            };
            async move {
                if let Some(block) = tiered_cache.get(&(sst.id(), block_index)).await? {
                    return Ok(block.into_owned());
                }

                let block_meta = sst.block_meta(block_index).ok_or_else(|| {
                    Error::Other(format!(
                        "invalid block idx: [sst: {}], [block: {}]",
                        sst.id(),
                        block_index
                    ))
                })?;
                let block_data = object_store
                    .get_range(&data_path, block_meta.data_range())
                    .await?
                    .ok_or_else(|| {
                        Error::ObjectStoreError(ObjectStoreError::ObjectNotFound(data_path))
                    })?;
                let block = Block::decode(&block_data)?;

                Ok(Box::new(block))
            }
        };

        match policy {
            CachePolicy::Fill => {
                self.block_cache
                    .get_or_insert_with(sst.id(), block_index, fetch_block)
                    .await
            }
            CachePolicy::NotFill => match self.block_cache.get(sst.id(), block_index) {
                Some(block) => Ok(block),
                None => fetch_block().await.map(BlockHolder::from_owned_block),
            },
            CachePolicy::Disable => fetch_block().await.map(BlockHolder::from_owned_block),
        }
    }

    pub async fn sstable(&self, sst_id: u64) -> Result<Sstable> {
        let meta = self.meta(sst_id).await?;
        Ok(Sstable::new(sst_id, meta))
    }

    async fn meta(&self, sst_id: u64) -> Result<Arc<SstableMeta>> {
        if let Some(meta) = self.meta_cache.get(&sst_id) {
            return Ok(meta);
        }
        let path = self.meta_path(sst_id);
        let buf = self
            .object_store
            .get(&path)
            .await?
            .ok_or_else(|| Error::ObjectStoreError(ObjectStoreError::ObjectNotFound(path)))?;
        let meta = Arc::new(SstableMeta::decode(&mut &buf[..]));
        self.meta_cache.insert(sst_id, meta.clone()).await;
        Ok(meta)
    }

    pub fn meta_path(&self, sst_id: u64) -> String {
        format!("{}/{}.meta", self.path, sst_id)
    }

    pub fn data_path(&self, sst_id: u64) -> String {
        format!("{}/{}.data", self.path, sst_id)
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.object_store.clone()
    }
}

pub type SstableStoreRef = Arc<SstableStore>;

#[cfg(test)]
mod tests {

    use runkv_common::coding::CompressionAlgorithm;
    use test_log::test;

    use super::*;
    use crate::components::{LsmTreeMetrics, SstableBuilder, SstableBuilderOptions};
    use crate::lsm_tree::TEST_DEFAULT_RESTART_INTERVAL;
    use crate::MemObjectStore;

    fn build_sstable_for_test() -> (SstableMeta, Vec<u8>) {
        let options = SstableBuilderOptions {
            capacity: 1024,
            block_capacity: 32,
            restart_interval: TEST_DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let mut builder = SstableBuilder::new(options);
        builder.add(b"k01", 1, Some(b"v01")).unwrap();
        builder.add(b"k02", 2, Some(b"v02")).unwrap();
        builder.add(b"k04", 4, Some(b"v04")).unwrap();
        builder.add(b"k05", 5, Some(b"v05")).unwrap();
        builder.build().unwrap()
    }

    #[test(tokio::test)]
    async fn test_sstable_store() {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(65536, Arc::new(LsmTreeMetrics::new(0)));
        let options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 1024,
            tiered_cache: TieredCache::none(),
        };
        let sstable_store = SstableStore::new(options);
        let (meta, data) = build_sstable_for_test();
        let meta = Arc::new(meta);
        let sst = Sstable::new(1, meta.clone());
        sstable_store
            .put(&sst, data.clone(), CachePolicy::Fill)
            .await
            .unwrap();
        // Check meta.
        let fetched_meta = sstable_store.meta(1).await.unwrap();
        assert_eq!(fetched_meta, meta);
        // Test fetch from block cache.
        for (block_idx, block_meta) in sst.block_metas_iter().enumerate() {
            let block = sstable_store
                .block(&sst, block_idx, CachePolicy::Fill)
                .await
                .unwrap();
            let origin_block = Block::decode(&data[block_meta.data_range()]).unwrap();
            assert_eq!(origin_block.data(), block.data());
        }
        // Test fetch from object store.
        for (block_idx, block_meta) in sst.block_metas_iter().enumerate() {
            let block = sstable_store
                .block(&sst, block_idx, CachePolicy::Disable)
                .await
                .unwrap();
            let origin_block = Block::decode(&data[block_meta.data_range()]).unwrap();
            assert_eq!(origin_block.data(), block.data());
        }
    }
}
