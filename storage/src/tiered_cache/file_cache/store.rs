// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use itertools::Itertools;
use nix::sys::statfs::{
    statfs, FsType as NixFsType, BTRFS_SUPER_MAGIC, EXT4_SUPER_MAGIC, TMPFS_MAGIC,
};
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::Instrument;

use super::error::{Error, Result};
use super::file::{CacheFile, CacheFileOptions};
use super::meta::{BlockLoc, MetaFile, SlotId};
use super::metrics::FileCacheMetricsRef;
use super::{utils, DioBuffer, DIO_BUFFER_ALLOCATOR};
use crate::tiered_cache::{HashBuilder, TieredCacheKey, TieredCacheValue};
use crate::utils::lru_cache::{LruCache, LruCacheEventListener};

const META_FILE_FILENAME: &str = "meta";
const CACHE_FILE_FILENAME: &str = "cache";

const FREELIST_DEFAULT_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Xfs,
    Ext4,
    Btrfs,
    Tmpfs,
}

pub struct StoreBatchWriter<'a, K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    keys: Vec<K>,
    /// `buffers` are fragmented by [`WRITER_MAX_IO_SIZE`].
    buffers: Vec<DioBuffer>,
    blocs: Vec<BlockLoc>,
    data_len: usize,

    block_size: usize,
    buffer_capacity: usize,
    max_write_size: usize,

    store: &'a Store<K, V>,
}

impl<'a, K, V> StoreBatchWriter<'a, K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    fn new(
        store: &'a Store<K, V>,
        block_size: usize,
        buffer_capacity: usize,
        item_capacity: usize,
        max_write_size: usize,
    ) -> Self {
        Self {
            keys: Vec::with_capacity(item_capacity),
            buffers: Vec::with_capacity(item_capacity),
            blocs: Vec::with_capacity(item_capacity),
            data_len: 0,

            block_size,
            buffer_capacity,
            max_write_size,

            store,
        }
    }

    #[allow(clippy::uninit_vec)]
    pub fn append<'b>(&'b mut self, key: K, value: &V) {
        let offset = self.data_len;
        let len = value.encoded_len();
        let bloc = BlockLoc {
            bidx: offset as u32 / self.block_size as u32,
            len: len as u32,
        };
        self.blocs.push(bloc);

        let rotate_last_mut = |buffers: &'b mut Vec<_>| {
            buffers.push(DioBuffer::with_capacity_in(
                self.buffer_capacity,
                &DIO_BUFFER_ALLOCATOR,
            ));
            buffers.last_mut().unwrap()
        };

        let buffer = match self.buffers.last_mut() {
            Some(buffer) if buffer.len() + len > self.max_write_size => {
                rotate_last_mut(&mut self.buffers)
            }
            None => rotate_last_mut(&mut self.buffers),
            Some(buffer) => buffer,
        };

        let buffer_offset = buffer.len();
        let buffer_len = utils::align_up(self.block_size, len);
        buffer.reserve(buffer_offset + buffer_len);
        unsafe {
            buffer.set_len(buffer_offset + buffer_len);
        }
        value.encode(&mut buffer[buffer_offset..buffer_offset + buffer_len]);
        self.data_len += buffer_len;

        self.keys.push(key);
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.blocs.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[tracing::instrument(skip(self))]
    pub async fn finish(mut self) -> Result<(Vec<K>, Vec<SlotId>)> {
        // First free slots during last batch.

        let mut freelist = Vec::with_capacity(FREELIST_DEFAULT_CAPACITY);
        std::mem::swap(&mut *self.store.freelist.write(), &mut freelist);

        if !freelist.is_empty() {
            let mut guard = self
                .store
                .meta_file
                .write()
                .instrument(tracing::trace_span!("meta_write_lock_free_slots"))
                .await;
            for slot in freelist {
                if let Some(bloc) = guard.free(slot) {
                    let offset = bloc.bidx as u64 * self.block_size as u64;
                    let len = bloc.blen(self.block_size as u32) as usize;
                    self.store.cache_file.punch_hole(offset, len)?;
                }
            }
            drop(guard);
        }

        if self.is_empty() {
            return Ok((vec![], vec![]));
        }

        // Write new cache entries.

        let mut slots = Vec::with_capacity(self.blocs.len());

        let mut boff: Option<u32> = None;

        for buffer in self.buffers {
            let blen = buffer.len();

            let timer = self.store.metrics.disk_write_latency.start_timer();
            let offset = self.store.cache_file.append(buffer).await? / self.block_size as u64;
            timer.observe_duration();

            if boff.is_none() {
                boff = Some(offset.try_into().unwrap());
            }

            self.store.metrics.disk_write_bytes.inc_by(blen as f64);
            self.store.metrics.disk_write_io_size.observe(blen as f64);
        }

        let boff = boff.unwrap();

        for bloc in &mut self.blocs {
            bloc.bidx += boff;
        }

        // Write guard is only needed when updating meta file, for data file is append-only.
        let mut guard = self
            .store
            .meta_file
            .write()
            .instrument(tracing::trace_span!("meta_write_lock_update_slots"))
            .await;

        for (key, bloc) in self.keys.iter().zip_eq(self.blocs.iter()) {
            slots.push(guard.insert(key, bloc)?);
        }

        Ok((self.keys, slots))
    }
}

pub struct StoreOptions {
    pub dir: String,
    pub capacity: usize,
    pub buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,
    pub cache_meta_fallocate_unit: usize,
    pub cache_file_max_write_size: usize,

    pub metrics: FileCacheMetricsRef,
}

pub struct Store<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    dir: String,
    _capacity: usize,

    fs_type: FsType,
    _fs_block_size: usize,
    block_size: usize,
    buffer_capacity: usize,
    cache_file_max_write_size: usize,

    meta_file: Arc<AsyncRwLock<MetaFile<K>>>,
    cache_file: CacheFile,

    freelist: Arc<RwLock<Vec<SlotId>>>,

    metrics: FileCacheMetricsRef,

    _phantom: PhantomData<V>,
}

impl<K, V> Store<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    pub async fn open(options: StoreOptions) -> Result<Self> {
        if !PathBuf::from(options.dir.as_str()).exists() {
            std::fs::create_dir_all(options.dir.as_str())?;
        }

        // Get file system type and block size by `statfs(2)`.
        let fs_stat = statfs(options.dir.as_str())?;
        let fs_type = match fs_stat.filesystem_type() {
            // FYI: https://github.com/nix-rust/nix/issues/1742
            // FYI: Aftere https://github.com/nix-rust/nix/pull/1743 is release,
            //      we can bump to the new nix version and use nix type instead of libc's.
            NixFsType(libc::XFS_SUPER_MAGIC) => FsType::Xfs,
            EXT4_SUPER_MAGIC => FsType::Ext4,
            BTRFS_SUPER_MAGIC => FsType::Btrfs,
            TMPFS_MAGIC => FsType::Tmpfs,
            nix_fs_type => return Err(Error::UnsupportedFilesystem(nix_fs_type.0)),
        };
        let fs_block_size = fs_stat.block_size() as usize;

        let cf_opts = CacheFileOptions {
            // TODO: Make it configurable.
            block_size: fs_block_size,
            fallocate_unit: options.cache_file_fallocate_unit,
        };

        let mf = MetaFile::open(
            PathBuf::from(&options.dir).join(META_FILE_FILENAME),
            options.cache_meta_fallocate_unit,
        )?;

        let cf = CacheFile::open(
            PathBuf::from(&options.dir).join(CACHE_FILE_FILENAME),
            cf_opts,
        )
        .await?;

        Ok(Self {
            dir: options.dir,
            _capacity: options.capacity,

            fs_type,
            _fs_block_size: fs_block_size,
            // TODO: Make it configurable.
            block_size: fs_block_size,
            buffer_capacity: options.buffer_capacity,
            cache_file_max_write_size: options.cache_file_max_write_size,

            meta_file: Arc::new(AsyncRwLock::new(mf)),
            cache_file: cf,

            freelist: Arc::new(RwLock::new(Vec::with_capacity(FREELIST_DEFAULT_CAPACITY))),

            metrics: options.metrics,

            _phantom: PhantomData::default(),
        })
    }

    pub fn fs_type(&self) -> FsType {
        self.fs_type
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub async fn size(&self) -> usize {
        self.cache_file.size() + self.meta_file.read().await.size()
    }

    pub async fn meta_file_size(&self) -> usize {
        self.meta_file.read().await.size()
    }

    pub fn cache_file_size(&self) -> usize {
        self.cache_file.size()
    }

    pub fn cache_file_len(&self) -> usize {
        self.cache_file.len()
    }

    pub fn meta_file_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(META_FILE_FILENAME)
    }

    pub fn cache_file_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(CACHE_FILE_FILENAME)
    }

    pub async fn restore<S: HashBuilder>(
        &self,
        indices: &Arc<LruCache<K, SlotId>>,
        hash_builder: &S,
    ) -> Result<()> {
        let slots = self.meta_file.read().await.slots();

        for slot in 0..slots {
            // Wrap the read guard, or there will be deadlock when evicting entries.
            let res = { self.meta_file.read().await.get(slot) };
            if let Some((block_loc, key)) = res {
                indices.insert(
                    key.clone(),
                    hash_builder.hash_one(&key),
                    utils::align_up(self.block_size, block_loc.len as usize),
                    slot,
                );
            }
        }

        Ok(())
    }

    pub fn start_batch_writer(&self, item_capacity: usize) -> StoreBatchWriter<'_, K, V> {
        StoreBatchWriter::new(
            self,
            self.block_size,
            self.buffer_capacity,
            item_capacity,
            self.cache_file_max_write_size,
        )
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&self, slot: SlotId) -> Result<Vec<u8>> {
        // Read guard should be held during reading meta and loading data.
        let guard = self
            .meta_file
            .read()
            .instrument(tracing::trace_span!("meta_file_read_lock"))
            .await;

        let (bloc, _key) = guard.get(slot).ok_or_else(|| Error::InvalidSlot(slot))?;
        let offset = bloc.bidx as u64 * self.block_size as u64;
        let blen = bloc.blen(self.block_size as u32) as usize;

        let timer = self.metrics.disk_read_latency.start_timer();
        let buf = self.cache_file.read(offset, blen).await?;
        timer.observe_duration();
        self.metrics.disk_read_bytes.inc_by(buf.len() as f64);
        self.metrics.disk_read_io_size.observe(buf.len() as f64);

        drop(guard);

        Ok(buf[..bloc.len as usize].to_vec())
    }

    pub fn erase(&self, slot: SlotId) -> Result<()> {
        self.free(slot)
    }

    fn free(&self, slot: SlotId) -> Result<()> {
        // `free` is called by `CacheableEntry::drop` which cannot be async, so delay actual free
        // until next batch write.

        // TODO(MrCroxx): Optimize freelist to use lock-free queue with batching read.
        self.freelist.write().push(slot);
        Ok(())
    }
}

impl<K, V> LruCacheEventListener for Store<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    type K = K;
    type T = SlotId;

    fn on_release(&self, _key: Self::K, slot: Self::T) {
        // TODO: Throw warning log instead?
        self.free(slot).unwrap();
    }
}

pub type StoreRef<K, V> = Arc<Store<K, V>>;

#[cfg(test)]
mod tests {

    use super::super::test_utils::{TestCacheKey, TestCacheValue};
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<StoreRef<TestCacheKey, TestCacheValue>>();
    }
}
