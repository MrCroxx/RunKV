use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use bytesize::ByteSize;
use parking_lot::RwLock;

use crate::components::{BlockCache, Memtable, SstableStore, SstableStoreOptions, SstableStoreRef};
use crate::iterator::{Iterator, Seek, SstableIterator, UserKeyIterator};
use crate::manifest::{LevelOptions, VersionManager, VersionManagerOptions};
use crate::utils::SKIPLIST_NODE_TOWER_MAX_HEIGHT;
use crate::{ObjectStoreRef, Result};

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

pub struct S3LsmTree {
    /// Options.
    options: S3LsmTreeOptions,
    /// Current mutable memtable.
    memtable: RefCell<Memtable>,
    /// Immutable memtabls, waiting to be uploaded to S3.
    immutable_memtables: RefCell<VecDeque<Memtable>>,
    version_manager: RwLock<VersionManager>,
    sstable_store: SstableStoreRef,
    /// TODO: Get from TSO.
    timestamp: AtomicU64,
    rwlock: RwLock<PhantomData<Memtable>>,
}

impl S3LsmTree {
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
            version_manager: RwLock::new(version_manager),
            sstable_store,
            timestamp: AtomicU64::new(0),
            rwlock: RwLock::new(PhantomData),
        }
    }

    pub async fn put(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        // TODO: Increase timestamp!
        // TODO: TODO: Get / Set timestamp from TSO.

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
            // TODO: Romove me.
            println!("rotate!");
            let imm = self.memtable.borrow().clone();
            *self.memtable.borrow_mut() =
                Memtable::new(self.options.meta_cache_capacity.0 as usize);
            self.immutable_memtables.borrow_mut().push_front(imm);
            drop(guard);
        }

        let ts = self.timestamp.fetch_add(1, Ordering::SeqCst) + 1;
        self.memtable.borrow_mut().put(key, value, ts);
        Ok(())
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, None).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // TODO: Pin timestamp!
        // TODO: TODO: Get / Set timestamp from TSO.

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

        let ts = self.timestamp.load(Ordering::SeqCst);

        // Seek from memtables.
        for memtable in memtables.iter() {
            if let Some(value) = memtable.get(key, ts) {
                return Ok(Some(Bytes::from(value.to_vec())));
            }
        }

        // Pick overlap ssts.
        let levels = {
            let version_manager_guard = self.version_manager.read();
            let key = Bytes::copy_from_slice(key);
            let levels = version_manager_guard
                .pick_overlap_ssts_by_key(0..version_manager_guard.levels(), &key, ts)
                .await
                .unwrap();
            drop(version_manager_guard);
            levels
        };

        // Seek from ssts.
        for level in levels {
            for sst_ids in level {
                let sst = self.sstable_store.sstable(sst_ids).await?;
                let mut iter = UserKeyIterator::new(
                    Box::new(SstableIterator::new(
                        self.sstable_store.clone(),
                        sst,
                        crate::components::CachePolicy::Fill,
                    )),
                    ts,
                );
                iter.seek(Seek::RandomForward(key)).await?;
                if iter.is_valid() && iter.key() == key {
                    return Ok(Some(Bytes::from(iter.value().to_vec())));
                }
            }
        }
        Ok(None)
    }
}

pub type S3LsmTreeRef = Arc<S3LsmTree>;

unsafe impl Send for S3LsmTree {}
unsafe impl Sync for S3LsmTree {}

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
        is_send_sync::<S3LsmTree>();
        is_send_sync::<S3LsmTree>();
    }

    #[tokio::test]
    async fn test_concurrent_put() {
        // Insert multiple kvs out of order concurrently.
        // Then iterate from start to end to check if memtable is complete and ordered.
        // Then check if get returns correct results on a timestamp while inserting kvs
        // concurrently.

        let lsmtree = default_s3_lsm_tree_options_for_test();
        let lsmtree = Arc::new(lsmtree);
        let futures = (1..=10000)
            .map(|i| {
                let lsmtree_clone = lsmtree.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    lsmtree_clone
                        .put(
                            format!("k{:08}", i).as_bytes(),
                            Some(format!("v{:08}", i).as_bytes()),
                        )
                        .await
                        .unwrap();
                    lsmtree_clone
                        .delete(format!("k{:08}", i).as_bytes())
                        .await
                        .unwrap();
                    lsmtree_clone
                        .put(
                            format!("k{:08}", i).as_bytes(),
                            Some(format!("v{:08}", i).as_bytes()),
                        )
                        .await
                        .unwrap();
                }
            })
            .collect_vec();
        future::join_all(futures).await;
        // let mut iter = MemtableIterator::new(&memtable, u64::MAX);
        // iter.seek(Seek::First).await.unwrap();
        // for i in 1..=10000 {
        //     assert_eq!(iter.key(), format!("k{:08}", i).as_bytes());
        //     iter.next().await.unwrap();
        // }
        // assert!(!iter.is_valid());

        // let put_futures = (1..=10000)
        //     .map(|i| {
        //         let memtable_clone = memtable.clone();
        //         async move {
        //             let mut rng = thread_rng();
        //             tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
        //             memtable_clone.put(
        //                 format!("k{:08}", i).as_bytes(),
        //                 Some(format!("v{:08}", i).as_bytes()),
        //                 i * 4 + 3,
        //             );
        //         }
        //     })
        //     .collect_vec();
        // let get_futures = (1..=10000)
        //     .map(|i| {
        //         let memtable_clone = memtable.clone();
        //         async move {
        //             let mut rng = thread_rng();
        //             tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
        //             let v = memtable_clone.get(format!("k{:08}", i).as_bytes(), i * 4 + 2);
        //             assert_eq!(v, Some(format!("v{:08}", i).as_bytes()));
        //         }
        //     })
        //     .collect_vec();
        // let put_future = future::join_all(put_futures);
        // let get_future = future::join_all(get_futures);
        // future::join(put_future, get_future).await;
    }

    fn default_s3_lsm_tree_options_for_test() -> S3LsmTree {
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
        S3LsmTree::new(options)
    }
}
