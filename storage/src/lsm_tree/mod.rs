pub mod components;
pub mod iterator;
pub mod manifest;
pub mod object_store_lsm_tree;
pub mod sstable_uploader;
pub mod utils;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Result;

const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64 KiB
const DEFAULT_RESTART_INTERVAL: usize = 16;
const TEST_DEFAULT_RESTART_INTERVAL: usize = 2;
const DEFAULT_ENTRY_SIZE: usize = 1024; // 1 KiB
const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;
const DEFAULT_SSTABLE_META_SIZE: usize = 4 * 1024; // 4 KiB
#[cfg(test)]
const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

// TODO: Support iterator on [`LsmTree`].
#[async_trait]
pub trait LsmTree: Send + Sync + Clone {
    /// Put a new `key` `value` pair into LSM-Tree with given `timestamp`.
    ///
    /// # Safety
    ///
    /// The interface exposes `timestamp` to user for the compatibility with upper system. It's
    /// caller's responsibility to ensure that the new timestamp is higher than the old one on the
    /// same key. Otherwise there will be consistency problems.
    async fn put(&self, key: &Bytes, value: &Bytes, timestamp: u64) -> Result<()>;

    /// Delete a the given `key` in LSM-Tree by tombstone with given `timestamp`.
    ///
    /// # Safety
    ///
    /// The interface exposes `timestamp` to user for the compatibility with upper system. It's
    /// caller's responsibility to ensure that the new timestamp is higher than the old one on the
    /// same key. Otherwise there will be consistency problems.
    async fn delete(&self, key: &Bytes, timestamp: u64) -> Result<()>;

    /// Get the value of the given `key` in LSM-Tree with given `timestamp`.
    ///
    /// # Safety
    ///
    /// The interface exposes `timestamp` to user for the compatibility with upper system. It's
    /// caller's responsibility to ensure that the new timestamp is higher than the old one on the
    /// same key. Otherwise there will be consistency problems.
    async fn get(&self, key: &Bytes, timestamp: u64) -> Result<Option<Bytes>>;
}
