use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use runkv_proto::meta::KeyRange;

use crate::error::Result;

pub mod mem;
#[allow(dead_code)]
pub mod object;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Update exhauster meta.
    async fn update_exhauster(&self, node_id: u64) -> Result<()>;

    /// Random pick a available exhauster.
    async fn pick_exhauster(&self, live: Duration) -> Result<Option<u64>>;

    /// Update responsable key ranges of wheel node.
    async fn update_node_ranges(&self, node_id: u64, ranges: Vec<KeyRange>) -> Result<()>;

    /// Get all responsable key ranges grouped by nodes.
    async fn all_node_ranges(&self) -> Result<BTreeMap<u64, Vec<KeyRange>>>;

    /// Get all responsable key ranges.
    async fn all_ranges(&self) -> Result<Vec<KeyRange>>;

    /// Pin sstables to prevent them from being compacted.
    ///
    /// Returns `true` if there is no conflicts and given sstables are pinned.
    async fn pin_sstables(&self, sst_ids: &[u64], time: SystemTime) -> Result<bool>;

    /// Unpin sstables no matter if they were pinned before.
    async fn unpin_sstables(&self, sst_ids: &[u64]) -> Result<()>;

    /// Check if sstables are pinned. Return a vector of pinned status.
    async fn is_sstables_pinned(&self, sst_ids: &[u64], time: SystemTime) -> Result<Vec<bool>>;

    /// Get the current timestamp.
    async fn timestamp(&self) -> Result<u32>;

    /// Fetch the current timestamp and advance it by `add`.
    async fn timestamp_fetch_add(&self, add: u32) -> Result<u32>;
}

pub type MetaStoreRef = Arc<dyn MetaStore>;
