pub mod object_store_lsm_tree;
pub mod sstable_uploader;
pub mod wheel_version_manager;

use async_trait::async_trait;
use bytes::Bytes;
pub use object_store_lsm_tree::*;
pub use sstable_uploader::*;
pub use wheel_version_manager::*;

use crate::error::Result;

// TODO: Support iterator on [`LsmTree`].
#[async_trait]
pub trait LsmTree: Send + Sync + Clone + 'static {
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
