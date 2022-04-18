use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use runkv_common::config::LevelCompactionStrategy;
use runkv_storage::components::{
    CachePolicy, Memtable, SstableStoreRef, SKIPLIST_NODE_TOWER_MAX_HEIGHT,
};
use runkv_storage::iterator::{Iterator, MergeIterator, Seek, SstableIterator, UserKeyIterator};
use runkv_storage::manifest::VersionManager;
use runkv_storage::utils::value;
use runkv_storage::{LsmTree, Result};
use tracing::trace;

#[derive(Clone)]
pub struct ObjectStoreLsmTreeOptions {
    /// Reference of sstable store.
    pub sstable_store: SstableStoreRef,
    /// Memtable capacity.
    pub write_buffer_capacity: usize,
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
        let memtable = RefCell::new(Memtable::new(options.write_buffer_capacity));
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
        for (i, memtable) in memtables.iter().enumerate() {
            trace!("find key {:?} in memtable {}", key, i);
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

        trace!("find key {:?} in ssts:\n{:?}", key, levels);

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
                            CachePolicy::Fill,
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
                            CachePolicy::Fill,
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

        // println!(
        //     "remain:{} approximate:{}",
        //     self.memtable.borrow().mem_remain(),
        //     approximate_size
        // );

        // Rotate active memtable within lock guard.
        if self.memtable.borrow().mem_remain() < approximate_size {
            let guard = self.rwlock.write();
            // println!("rotate memtable");
            let imm = self.memtable.borrow().clone();
            *self.memtable.borrow_mut() = Memtable::new(self.options.write_buffer_capacity);
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
pub mod tests {
    use std::sync::Arc;

    use runkv_common::coding::CompressionAlgorithm;
    use runkv_common::config::LevelOptions;
    use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions};
    use runkv_storage::manifest::VersionManagerOptions;
    use runkv_storage::MemObjectStore;

    use super::*;

    pub fn build_test_lsm_tree() -> ObjectStoreLsmTree {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(16 << 10);
        let sstable_store = Arc::new(SstableStore::new(SstableStoreOptions {
            path: "path".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 4 << 10,
        }));
        let version_manager = VersionManager::new(VersionManagerOptions {
            levels_options: vec![
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::Overlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
            ],
            levels: vec![vec![]; 2],
            sstable_store: sstable_store.clone(),
        });
        ObjectStoreLsmTree::new(ObjectStoreLsmTreeOptions {
            sstable_store,
            write_buffer_capacity: 4 << 10,
            version_manager,
        })
    }
}
