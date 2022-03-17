use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
use parking_lot::RwLock;

use crate::components::{BlockCache, Memtable, SstableStore, SstableStoreOptions, SstableStoreRef};
use crate::iterator::{Iterator, MergeIterator, Seek, SstableIterator, UserKeyIterator};
use crate::manifest::{
    LevelCompactionStrategy, LevelOptions, VersionManager, VersionManagerOptions,
};
use crate::utils::{value, SKIPLIST_NODE_TOWER_MAX_HEIGHT};
use crate::{LsmTree, ObjectStoreRef, Result};

#[derive(Clone)]
pub struct S3LsmTreeOptions {
    /// Data directory path on S3.
    pub path: String,
    /// Object store client.
    pub object_store: ObjectStoreRef,
    /// Memtable capacity.
    pub write_buffer_capacity: ByteSize,
    /// Block cache capacity.
    pub block_cache_capacity: ByteSize,
    /// Meta cache capacity.
    pub meta_cache_capacity: ByteSize,
    /// Options for eash level of LSM-Tree.
    pub lsm_tree_level_options: Vec<LevelOptions>,
}

pub struct S3LsmTreeCore {
    /// Options.
    options: S3LsmTreeOptions,
    /// Current mutable memtable.
    memtable: RefCell<Memtable>,
    /// Immutable memtabls, waiting to be uploaded to S3.
    immutable_memtables: RefCell<VecDeque<Memtable>>,
    version_manager: tokio::sync::RwLock<VersionManager>,
    sstable_store: SstableStoreRef,
    /// Use a external lock to tighten the critical section.
    rwlock: RwLock<PhantomData<Memtable>>,
}

impl S3LsmTreeCore {
    pub fn new(options: S3LsmTreeOptions) -> Self {
        let sstable_store_options = SstableStoreOptions {
            path: options.path.clone(),
            object_store: options.object_store.clone(),
            block_cache: BlockCache::new(options.block_cache_capacity.0 as usize),
            meta_cache_capacity: options.meta_cache_capacity.0 as usize,
        };
        let sstable_store = Arc::new(SstableStore::new(sstable_store_options));
        let memtable = RefCell::new(Memtable::new(options.write_buffer_capacity.0 as usize));
        let immutable_memtables = RefCell::new(VecDeque::with_capacity(32));
        // TODO: Recover sstable levels.
        let levels = vec![vec![]; options.lsm_tree_level_options.len()];
        let version_manager_options = VersionManagerOptions {
            level_options: options.lsm_tree_level_options.clone(),
            levels,
            sstable_store: sstable_store.clone(),
        };
        let version_manager = VersionManager::new(version_manager_options);
        Self {
            options,
            memtable,
            immutable_memtables,
            version_manager: tokio::sync::RwLock::new(version_manager),
            sstable_store,
            rwlock: RwLock::new(PhantomData),
        }
    }

    async fn put(&self, key: &Bytes, value: &Bytes, timestamp: u64) -> Result<()> {
        self.write(key, Some(value), timestamp).await
    }

    async fn delete(&self, key: &Bytes, timestamp: u64) -> Result<()> {
        self.write(key, None, timestamp).await
    }

    async fn get(&self, key: &Bytes, timestamp: u64) -> Result<Option<Bytes>> {
        // Prevent current memtable and immutable memtable vec being modified.
        let memtables = {
            let guard = self.rwlock.read();
            let imms = self.immutable_memtables.borrow();
            let mut memtables = Vec::with_capacity(imms.len() + 1);
            memtables.push(self.memtable.borrow().clone());
            memtables.extend(imms.clone().drain(..));
            drop(guard);
            memtables
        };

        // Seek from memtables.
        for memtable in memtables.iter() {
            if let Some(raw) = memtable.get_raw(key, timestamp) {
                return Ok(value(&raw).map(Bytes::copy_from_slice));
            }
        }

        // Pick overlap ssts.
        let levels = {
            let version_manager_guard = self.version_manager.read().await;
            let levels = version_manager_guard
                .pick_overlap_ssts_by_key(0..version_manager_guard.levels(), key, timestamp)
                .await
                .unwrap();
            drop(version_manager_guard);
            levels
        };

        // Seek from ssts.
        for (level_idx, level) in levels.into_iter().enumerate() {
            if level.is_empty() {
                continue;
            }
            let compaction_strategy = self
                .version_manager
                .read()
                .await
                .level_compaction_strategy(level_idx as u64)?;

            let mut iter = match compaction_strategy {
                LevelCompactionStrategy::Overlap => {
                    let mut iters: Vec<Box<dyn Iterator>> = Vec::with_capacity(level.len());
                    for sst_id in level {
                        let sst = self.sstable_store.sstable(sst_id).await?;
                        let iter = Box::new(SstableIterator::new(
                            self.sstable_store.clone(),
                            sst,
                            crate::components::CachePolicy::Fill,
                        ));
                        iters.push(iter);
                    }
                    UserKeyIterator::new(Box::new(MergeIterator::new(iters)), timestamp)
                }
                LevelCompactionStrategy::NonOverlap => {
                    assert_eq!(level.len(), 1);
                    let sst = self.sstable_store.sstable(level[0]).await?;
                    UserKeyIterator::new(
                        Box::new(SstableIterator::new(
                            self.sstable_store.clone(),
                            sst,
                            crate::components::CachePolicy::Fill,
                        )),
                        timestamp,
                    )
                }
            };
            if iter.seek(Seek::RandomForward(key)).await? {
                if iter.is_valid() && iter.key() == key {
                    return Ok(Some(Bytes::from(iter.value().to_vec())));
                } else {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    async fn write(&self, key: &Bytes, value: Option<&Bytes>, timestamp: u64) -> Result<()> {
        // = key len + value len
        // + skiplist node height (8B) + skiplist node tower (4 * MAX)
        // + reserved (8B)
        let approximate_size = key.len()
            + if let Some(v) = value.as_ref() {
                v.len()
            } else {
                0
            }
            + 8
            + 4 * SKIPLIST_NODE_TOWER_MAX_HEIGHT
            + 8;

        // Rotate active memtable within lock guard.
        if self.memtable.borrow().mem_remain() < approximate_size {
            let guard = self.rwlock.write();
            println!("rotate!");
            let imm = self.memtable.borrow().clone();
            *self.memtable.borrow_mut() =
                Memtable::new(self.options.meta_cache_capacity.0 as usize);
            self.immutable_memtables.borrow_mut().push_front(imm);
            drop(guard);
        }

        self.memtable.borrow_mut().put(key, value, timestamp);
        Ok(())
    }
}

// unsafe impl Send for S3LsmTreeCore {}
unsafe impl Sync for S3LsmTreeCore {}

pub struct S3LsmTree {
    inner: Arc<S3LsmTreeCore>,
}

#[async_trait]
impl LsmTree for S3LsmTree {
    async fn put(&self, key: &Bytes, value: &Bytes, timestamp: u64) -> Result<()> {
        self.inner.put(key, value, timestamp).await
    }

    async fn delete(&self, key: &Bytes, timestamp: u64) -> Result<()> {
        self.inner.delete(key, timestamp).await
    }

    async fn get(&self, key: &Bytes, timestamp: u64) -> Result<Option<Bytes>> {
        self.inner.get(key, timestamp).await
    }
}

impl S3LsmTree {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future;
    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use super::*;
    use crate::manifest::LevelCompactionStrategy;
    use crate::utils::CompressionAlgorighm;
    use crate::MemObjectStore;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<S3LsmTreeCore>();
        is_send_sync::<S3LsmTreeCore>();
    }

    #[tokio::test]
    async fn test_concurrent_put() {
        let lsmtree = default_s3_lsm_tree_options_for_test();
        let lsmtree = Arc::new(lsmtree);
        let futures = (1..=10000)
            .map(|i| {
                let lsmtree_clone = lsmtree.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    lsmtree_clone.put(&key(i), &value(i), i * 4).await.unwrap();
                    println!("put k{:08}", i);
                    assert_eq!(
                        lsmtree_clone.get(&key(i), i * 4).await.unwrap(),
                        Some(value(i))
                    );
                    lsmtree_clone.delete(&key(i), i * 4 + 1).await.unwrap();
                    println!("delete k{:08}", i);
                    assert_eq!(lsmtree_clone.get(&key(i), i * 4 + 1).await.unwrap(), None);
                    lsmtree_clone
                        .put(&key(i), &value(i), i * 4 + 2)
                        .await
                        .unwrap();
                    println!("put k{:08}", i);
                    assert_eq!(
                        lsmtree_clone.get(&key(i), i * 4 + 2).await.unwrap(),
                        Some(value(i))
                    );
                }
            })
            .collect_vec();
        future::join_all(futures).await;
    }

    fn default_s3_lsm_tree_options_for_test() -> S3LsmTreeCore {
        let object_store = Arc::new(MemObjectStore::default());
        let options = S3LsmTreeOptions {
            path: "test".to_string(),
            object_store,
            write_buffer_capacity: ByteSize(64 * 1024),
            block_cache_capacity: ByteSize(64 * 1024),
            meta_cache_capacity: ByteSize(1024 * 1024),
            lsm_tree_level_options: vec![
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
            ],
        };
        S3LsmTreeCore::new(options)
    }

    fn key(i: u64) -> Bytes {
        Bytes::from(format!("k{:08}", i))
    }

    fn value(i: u64) -> Bytes {
        Bytes::from(format!("v{:08}", i))
    }
}
