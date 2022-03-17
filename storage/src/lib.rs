#![feature(drain_filter)]
#![feature(assert_matches)]

mod error;
mod lsm_tree;
mod object_store;

use async_trait::async_trait;
use bytes::Bytes;
pub use error::*;
pub use lsm_tree::*;
pub use object_store::*;

#[async_trait]
pub trait LsmTree: Send + Sync {
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
