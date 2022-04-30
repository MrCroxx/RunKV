use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use runkv_common::config::LevelCompactionStrategy;
use runkv_storage::components::{
    CachePolicy, Memtable, SstableStoreRef, SKIPLIST_NODE_TOWER_MAX_HEIGHT,
};
use runkv_storage::iterator::{Iterator, MergeIterator, Seek, SstableIterator, UserKeyIterator};
use runkv_storage::manifest::VersionManager;
use runkv_storage::utils::value;
use runkv_storage::Result;
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

pub struct MemtableWithCtx {
    pub table: Memtable,
    pub max_applied_index: u64,
    pub max_sequence: u64,
}

impl MemtableWithCtx {
    fn new(capacity: usize) -> Self {
        Self {
            table: Memtable::new(capacity),
            max_applied_index: 0,
            max_sequence: 0,
        }
    }
}

struct Memtables {
    memtable: MemtableWithCtx,
    immutable_memtables: VecDeque<MemtableWithCtx>,
}

struct ObjectStoreLsmTreeCore {
    /// Options.
    options: ObjectStoreLsmTreeOptions,

    version_manager: VersionManager,
    sstable_store: SstableStoreRef,

    memtables: RwLock<Memtables>,
    // ----- critical section protected by rwlock -----
    // /// Current mutable memtable.
    // memtable: RefCell<Memtable>,
    // /// Immutable memtabls, waiting to be uploaded to S3.
    // immutable_memtables: RefCell<VecDeque<Memtable>>,
    // /// Use a external lock to tighten the critical section.
    // rwlock: RwLock<PhantomData<Memtable>>,
    // ----- critical section protected by rwlock -----
}

impl ObjectStoreLsmTreeCore {
    pub fn new(options: ObjectStoreLsmTreeOptions) -> Self {
        Self {
            version_manager: options.version_manager.clone(),
            sstable_store: options.sstable_store.clone(),

            memtables: RwLock::new(Memtables {
                memtable: MemtableWithCtx::new(options.write_buffer_capacity),
                immutable_memtables: VecDeque::with_capacity(32),
            }),

            options,
        }
    }

    async fn get(&self, key: &Bytes, sequence: u64) -> Result<Option<Bytes>> {
        // Prevent current memtable and immutable memtable vec being modified.
        let memtables = {
            let guard = self.memtables.read();
            let mut memtables = Vec::with_capacity(guard.immutable_memtables.len() + 1);
            memtables.push(guard.memtable.table.clone());
            memtables.extend(
                guard
                    .immutable_memtables
                    .iter()
                    .map(|memtable| &memtable.table)
                    .cloned(),
            );
            drop(guard);
            memtables
        };

        // Seek from memtables.
        for (i, memtable) in memtables.iter().enumerate() {
            trace!("find key {:?} in memtable {}", key, i);
            if let Some(raw) = memtable.get_raw(key, sequence) {
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
                    UserKeyIterator::new(Box::new(MergeIterator::new(iters)), sequence)
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
                        sequence,
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

    async fn write(
        &self,
        key: &Bytes,
        value: Option<&Bytes>,
        sequence: u64,
        apply_index: u64,
    ) -> Result<()> {
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

        let mut guard = self.memtables.write();
        // Rotate memtable if needed.
        if guard.memtable.table.mem_remain() < approximate_size {
            trace!("rotate memtable");
            let mut imm = MemtableWithCtx::new(self.options.write_buffer_capacity);
            std::mem::swap(&mut imm, &mut guard.memtable);
            guard.immutable_memtables.push_front(imm);
        }
        guard.memtable.table.put(key, value, sequence);
        trace!(
            "apply index: {}, max applied index: {}",
            apply_index,
            guard.memtable.max_applied_index,
        );
        assert!(apply_index >= guard.memtable.max_applied_index);
        guard.memtable.max_applied_index = apply_index;
        trace!(
            "sequence: {}, max sequence: {}",
            sequence,
            guard.memtable.max_sequence,
        );
        assert!(sequence >= guard.memtable.max_sequence);
        guard.memtable.max_sequence = sequence;
        drop(guard);

        Ok(())
    }

    fn get_oldest_immutable_memtable(&self) -> Option<Memtable> {
        self.memtables
            .read()
            .immutable_memtables
            .back()
            .map(|memtable| &memtable.table)
            .cloned()
    }

    fn drop_oldest_immutable_memtable(&self) -> MemtableWithCtx {
        let imm = self
            .memtables
            .write()
            .immutable_memtables
            .pop_back()
            .unwrap();
        imm
    }
}

unsafe impl Sync for ObjectStoreLsmTreeCore {}

#[derive(Clone)]
pub struct ObjectStoreLsmTree {
    inner: Arc<ObjectStoreLsmTreeCore>,
}

impl ObjectStoreLsmTree {
    pub fn new(options: ObjectStoreLsmTreeOptions) -> Self {
        Self {
            inner: Arc::new(ObjectStoreLsmTreeCore::new(options)),
        }
    }

    /// Put a new `key` `value` pair into LSM-Tree with given `sequence`.
    ///
    /// # Safety
    ///
    /// The interface exposes `sequence` to user for the compatibility with upper system. It's
    /// caller's responsibility to ensure that the new sequence is higher than the old one on the
    /// same key. Otherwise there will be consistency problems.
    pub async fn put(
        &self,
        key: &Bytes,
        value: &Bytes,
        sequence: u64,
        apply_index: u64,
    ) -> Result<()> {
        self.inner
            .write(key, Some(value), sequence, apply_index)
            .await
    }

    /// Delete a the given `key` in LSM-Tree by tombstone with given `sequence`.
    ///
    /// # Safety
    ///
    /// The interface exposes `sequence` to user for the compatibility with upper system. It's
    /// caller's responsibility to ensure that the new sequence is higher than the old one on the
    /// same key. Otherwise there will be consistency problems.
    pub async fn delete(&self, key: &Bytes, sequence: u64, apply_index: u64) -> Result<()> {
        self.inner.write(key, None, sequence, apply_index).await
    }

    /// Get the value of the given `key` in LSM-Tree with given `sequence`.
    ///
    /// # Safety
    ///
    /// The interface exposes `sequence` to user for the compatibility with upper system. It's
    /// caller's responsibility to ensure that the new sequence is higher than the old one on the
    /// same key. Otherwise there will be consistency problems.
    pub async fn get(&self, key: &Bytes, sequence: u64) -> Result<Option<Bytes>> {
        self.inner.get(key, sequence).await
    }

    pub fn get_oldest_immutable_memtable(&self) -> Option<Memtable> {
        self.inner.get_oldest_immutable_memtable()
    }

    pub fn drop_oldest_immutable_memtable(&self) -> MemtableWithCtx {
        self.inner.drop_oldest_immutable_memtable()
    }
}
