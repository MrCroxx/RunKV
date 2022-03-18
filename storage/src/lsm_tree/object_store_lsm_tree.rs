use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
use parking_lot::RwLock;

use crate::components::{Memtable, SstableStoreRef};
use crate::iterator::{Iterator, MergeIterator, Seek, SstableIterator, UserKeyIterator};
use crate::manifest::{LevelCompactionStrategy, VersionManager};
use crate::utils::{value, SKIPLIST_NODE_TOWER_MAX_HEIGHT};
use crate::{LsmTree, Result};

#[derive(Clone)]
pub struct ObjectStoreLsmTreeOptions {
    /// Reference of sstable store.
    pub sstable_store: SstableStoreRef,
    /// Memtable capacity.
    pub write_buffer_capacity: ByteSize,
    /// Block cache capacity.
    pub block_cache_capacity: ByteSize,
    /// Meta cache capacity.
    pub meta_cache_capacity: ByteSize,
    /// Local version manager.
    pub version_manager: VersionManager,
}

pub struct ObjectStoreLsmTreeCore {
    /// Options.
    options: ObjectStoreLsmTreeOptions,
    /// Current mutable memtable.
    memtable: RefCell<Memtable>,
    /// Immutable memtabls, waiting to be uploaded to S3.
    immutable_memtables: RefCell<VecDeque<Memtable>>,
    version_manager: VersionManager,
    sstable_store: SstableStoreRef,
    /// Use a external lock to tighten the critical section.
    rwlock: RwLock<PhantomData<Memtable>>,
}

impl ObjectStoreLsmTreeCore {
    pub fn new(options: ObjectStoreLsmTreeOptions) -> Self {
        let memtable = RefCell::new(Memtable::new(options.write_buffer_capacity.0 as usize));
        let immutable_memtables = RefCell::new(VecDeque::with_capacity(32));
        Self {
            memtable,
            immutable_memtables,
            version_manager: options.version_manager.clone(),
            sstable_store: options.sstable_store.clone(),
            rwlock: RwLock::new(PhantomData),
            options,
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
            // println!("find key {:?} in memtable {}", key, i);
            if let Some(raw) = memtable.get_raw(key, timestamp) {
                return Ok(value(&raw).map(Bytes::copy_from_slice));
            }
        }

        // Pick overlap ssts.
        let levels = {
            let levels = self
                .version_manager
                .pick_overlap_ssts_by_key(0..self.version_manager.levels().await, key)
                .await
                .unwrap();
            levels
        };

        // println!("find key {:?} in ssts:\n{:?}", key, levels);

        // Seek from ssts.
        for (level_idx, level) in levels.into_iter().enumerate() {
            if level.is_empty() {
                continue;
            }
            let compaction_strategy = self
                .version_manager
                .level_compaction_strategy(level_idx as u64)
                .await?;

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
            // println!("rotate memtable");
            let imm = self.memtable.borrow().clone();
            *self.memtable.borrow_mut() =
                Memtable::new(self.options.meta_cache_capacity.0 as usize);
            self.immutable_memtables.borrow_mut().push_front(imm);
            drop(guard);
        }

        self.memtable.borrow_mut().put(key, value, timestamp);
        Ok(())
    }

    fn get_oldest_immutable_memtable(&self) -> Option<Memtable> {
        let guard = self.rwlock.read();
        let imm = self.immutable_memtables.borrow().back().cloned();
        drop(guard);
        imm
    }

    fn drop_oldest_immutable_memtable(&self) {
        let guard = self.rwlock.write();
        let mut imms = self.immutable_memtables.borrow_mut();
        assert!(!imms.is_empty());
        imms.pop_back();
        drop(guard);
    }
}

unsafe impl Sync for ObjectStoreLsmTreeCore {}

#[derive(Clone)]
pub struct ObjectStoreLsmTree {
    inner: Arc<ObjectStoreLsmTreeCore>,
}

#[async_trait]
impl LsmTree for ObjectStoreLsmTree {
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

impl ObjectStoreLsmTree {
    pub fn new(options: ObjectStoreLsmTreeOptions) -> Self {
        Self {
            inner: Arc::new(ObjectStoreLsmTreeCore::new(options)),
        }
    }

    pub fn get_oldest_immutable_memtable(&self) -> Option<Memtable> {
        self.inner.get_oldest_immutable_memtable()
    }

    pub fn drop_oldest_immutable_memtable(&self) {
        self.inner.drop_oldest_immutable_memtable()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future;
    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use super::*;
    use crate::components::{BlockCache, SstableStore, SstableStoreOptions};
    use crate::manifest::{LevelCompactionStrategy, LevelOptions, VersionManagerOptions};
    use crate::uploader::{Uploader, UploaderOptions};
    use crate::utils::CompressionAlgorithm;
    use crate::MemObjectStore;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<ObjectStoreLsmTreeCore>();
        is_send_sync::<Uploader>();
    }

    #[tokio::test]
    async fn test_concurrent_put() {
        let lsmtree = default_lsm_tree_for_test().await;
        let futures = (1..=10000)
            .map(|i| {
                let lsmtree_clone = lsmtree.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    lsmtree_clone.put(&key(i), &value(i), 1).await.unwrap();
                    // println!("put {:?} at {}", key(i), 1);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    // println!("get {:?} at {}", key(i), 3);
                    assert_eq!(lsmtree_clone.get(&key(i), 3).await.unwrap(), Some(value(i)));
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    lsmtree_clone.delete(&key(i), 5).await.unwrap();
                    // println!("delete {:?} at {}", key(i), 5);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    // println!("get {:?} at {}", key(i), 7);
                    assert_eq!(lsmtree_clone.get(&key(i), 7).await.unwrap(), None);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    lsmtree_clone.put(&key(i), &value(i), 9).await.unwrap();
                    // println!("put {:?} at {}", key(i), 9);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    // println!("get {:?} at {}", key(i), 11);
                    assert_eq!(
                        lsmtree_clone.get(&key(i), 11).await.unwrap(),
                        Some(value(i))
                    );
                }
            })
            .collect_vec();
        future::join_all(futures).await;
        while lsmtree.get_oldest_immutable_memtable().is_some() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn default_lsm_tree_for_test() -> ObjectStoreLsmTree {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(65536);
        let sstable_store_options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 1024,
        };
        let sstable_store = Arc::new(SstableStore::new(sstable_store_options));
        let version_manager_options = VersionManagerOptions {
            levels_options: vec![
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::Overlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::Lz4,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::Lz4,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::Lz4,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::Lz4,
                },
            ],
            levels: vec![vec![]; 7],
            sstable_store: sstable_store.clone(),
        };

        let version_manager = VersionManager::new(version_manager_options);

        let lsm_tree_options = ObjectStoreLsmTreeOptions {
            sstable_store: sstable_store.clone(),
            write_buffer_capacity: ByteSize(64 * 1024),
            block_cache_capacity: ByteSize(64 * 1024),
            meta_cache_capacity: ByteSize(1024 * 1024),
            version_manager: version_manager.clone(),
        };
        let lsm_tree = ObjectStoreLsmTree::new(lsm_tree_options);

        let uploader_options = UploaderOptions {
            lsm_tree: lsm_tree.clone(),
            sstable_store,
            version_manager,
            sstable_capacity: ByteSize(16 * 1024),
            block_capacity: ByteSize(1024),
            restart_interval: 2,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
            poll_interval: Duration::from_millis(100),
        };
        let uploader = Uploader::new(uploader_options);
        tokio::spawn(async move { uploader.run().await });
        lsm_tree
    }

    fn key(i: u64) -> Bytes {
        Bytes::from(format!("k{:064}", i))
    }

    fn value(i: u64) -> Bytes {
        Bytes::from(format!("v{:064}", i))
    }
}
