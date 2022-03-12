use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;

use super::{Block, BlockCache, Sstable};
use crate::object_store::ObjectStoreRef;
use crate::{Error, Result, SstableMeta};

// TODO: Define policy based on use cases (read / comapction / ...).
pub enum CachePolicy {
    Disable,
    Fill,
    NotFill,
}

pub struct SstableStoreOptions {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache_capacity: usize,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Cache<u64, SstableMeta>,
}

impl SstableStore {
    pub fn new(options: SstableStoreOptions) -> Self {
        Self {
            path: options.path,
            store: options.store,
            block_cache: options.block_cache,
            meta_cache: Cache::new(options.meta_cache_capacity as u64),
        }
    }

    pub async fn put(&self, sst: &Sstable, data: Bytes, policy: CachePolicy) -> Result<()> {
        let data_path = self.get_sst_data_path(sst.id);
        self.store.put(&data_path, data.clone()).await?;

        let meta = sst.meta.encode();
        let meta_path = self.get_sst_meta_path(sst.id);
        if let Err(e) = self.store.put(&meta_path, meta).await {
            self.store.remove(&data_path).await?;
            return Err(e);
        }

        if let CachePolicy::Fill = policy {
            for (block_idx, meta) in sst.meta.block_metas.iter().enumerate() {
                let offset = meta.offset as usize;
                let len = meta.len as usize;
                let block = Arc::new(Block::decode(data.slice(offset..offset + len))?);
                self.block_cache
                    .insert(sst.id, block_idx as u64, block)
                    .await
            }
        }

        Ok(())
    }

    pub async fn block(
        &self,
        sst: &Sstable,
        block_index: u64,
        policy: CachePolicy,
    ) -> Result<Arc<Block>> {
        let fetch_block = async move {
            let block_meta = sst
                .meta
                .block_metas
                .get(block_index as usize)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "invalid block idx: [sst: {}], [block: {}]",
                        sst.id, block_index
                    ))
                })?;
            let data_path = self.get_sst_data_path(sst.id);
            let block_data = self
                .store
                .get_range(&data_path, block_meta.data_range())
                .await?;
            let block = Block::decode(block_data)?;
            Ok(Arc::new(block))
        };

        match policy {
            CachePolicy::Fill => {
                self.block_cache
                    .get_or_insert_with(sst.id, block_index, fetch_block)
                    .await
            }
            CachePolicy::NotFill => match self.block_cache.get(sst.id, block_index) {
                Some(block) => Ok(block),
                None => fetch_block.await,
            },
            CachePolicy::Disable => fetch_block.await,
        }
    }

    pub async fn meta(&self, sst_id: u64) -> Result<SstableMeta> {
        if let Some(meta) = self.meta_cache.get(&sst_id) {
            return Ok(meta);
        }
        let path = self.get_sst_meta_path(sst_id);
        let buf = self.store.get(&path).await?;
        let meta = SstableMeta::decode(buf);
        self.meta_cache.insert(sst_id, meta.clone()).await;
        Ok(meta)
    }

    pub fn get_sst_meta_path(&self, sst_id: u64) -> String {
        format!("{}/{}.meta", self.path, sst_id)
    }

    pub fn get_sst_data_path(&self, sst_id: u64) -> String {
        format!("{}/{}.data", self.path, sst_id)
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
