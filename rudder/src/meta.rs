use std::sync::Arc;

use runkv_storage::{SstableStoreRef, VersionManager};

use crate::{Error, Result};

pub struct MetaManagerOptions {
    pub version: VersionManager,
    pub watermark: u64,
    pub sstable_store: SstableStoreRef,
}

pub struct MetaManager {
    /// Manifest of sstables.
    _version_manager: VersionManager,
    /// The smallest pinned timestamp. Any data whose timestamp is smaller than `watermark` can be
    /// safely delete.
    ///
    /// `wheel node` maintains its own watermark, and `rudder node` collects watermarks from each
    /// `wheel node` periodically and choose the min watermark among them as its own watermark.
    watermark: u64,
    _sstable_store: SstableStoreRef,
}

impl MetaManager {
    pub fn new(options: MetaManagerOptions) -> Self {
        Self {
            _version_manager: options.version,
            watermark: options.watermark,
            _sstable_store: options.sstable_store,
        }
    }

    /// Advance watermark.
    pub fn advance(&mut self, wartermark: u64) -> Result<()> {
        if self.watermark > wartermark {
            return Err(Error::InvalidWatermark(self.watermark, wartermark));
        }
        self.watermark = wartermark;
        Ok(())
    }
}

pub type MetaManagerRef = Arc<MetaManager>;

#[cfg(test)]
mod tests {
    use runkv_storage::{
        BlockCache, CompressionAlgorighm, LevelCompactionStrategy, LevelOptions, MemObjectStore,
        SstableStore, SstableStoreOptions, VersionManagerOptions,
    };

    use super::*;

    fn is_send_sync<T: Send + Sync>() {}

    fn _build_meta_manager_for_test() -> MetaManagerRef {
        let level_options = vec![
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::Overlap,
                compression_algorighm: CompressionAlgorighm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
        ];
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(0);
        let sstable_store_options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 65536,
        };
        let sstable_store = Arc::new(SstableStore::new(sstable_store_options));
        let version_manager_options = VersionManagerOptions {
            level_options,
            levels: vec![vec![]; 7],
            sstable_store: sstable_store.clone(),
        };
        let version_manager = VersionManager::new(version_manager_options).unwrap();
        let meta_manager_options = MetaManagerOptions {
            version: version_manager,
            watermark: 0,
            sstable_store,
        };
        Arc::new(MetaManager::new(meta_manager_options))
    }

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<VersionManager>();
    }
}
