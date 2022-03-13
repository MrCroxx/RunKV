use std::sync::Arc;

use runkv_storage::{SstableStoreRef, Version};

use crate::{Error, Result};

pub struct VersionManagerOptions {
    pub version: Version,
    pub watermark: u64,
    pub sstable_store: SstableStoreRef,
}

pub struct VersionManager {
    /// Manifest of sstables.
    _version: Version,
    /// The smallest pinned timestamp. Any data whose timestamp is smaller than `watermark` can be
    /// safely delete.
    ///
    /// `wheel node` maintains its own watermark, and `rudder node` collects watermarks from each
    /// `wheel node` periodically and choose the min watermark among them as its own watermark.
    watermark: u64,
    _sstable_store: SstableStoreRef,
}

impl VersionManager {
    pub fn new(options: VersionManagerOptions) -> Self {
        Self {
            _version: options.version,
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

pub type VersionManagerRef = Arc<VersionManager>;

#[cfg(test)]
mod tests {
    use runkv_storage::{
        BlockCache, CompressionAlgorighm, LevelCompactionStrategy, LevelOptions, MemObjectStore,
        SstableStore, SstableStoreOptions,
    };

    use super::*;

    fn is_send_sync(_: impl Send + Sync) -> bool {
        true
    }

    #[test]
    fn ensure_sync() {
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
        let version = Version::new(level_options);
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(65536);
        let sstable_store_options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 1024,
        };
        let sstable_store = Arc::new(SstableStore::new(sstable_store_options));
        let options = VersionManagerOptions {
            version,
            watermark: 0,
            sstable_store,
        };
        let version_manager = Arc::new(VersionManager::new(options));
        assert!(is_send_sync(version_manager));
    }
}
