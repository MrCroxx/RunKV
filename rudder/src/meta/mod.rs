use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use runkv_proto::common::Endpoint;
use runkv_proto::meta::{KeyRange, KeyRangeInfo};
use runkv_proto::rudder::RaftState;

use crate::error::Result;

pub mod mem;
#[allow(dead_code)]
pub mod object;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Add new wheel.
    async fn add_wheels(&self, wheels: HashMap<u64, Endpoint>) -> Result<()>;

    /// Get all wheel ids.
    async fn wheels(&self) -> Result<HashMap<u64, Endpoint>>;

    /// Add new key range.
    async fn add_key_ranges(&self, key_ranges: Vec<KeyRangeInfo>) -> Result<()>;

    /// Get all key range infos.
    async fn all_key_range_infos(&self) -> Result<Vec<KeyRangeInfo>>;

    /// Update raft states.
    async fn update_raft_states(&self, raft_states: HashMap<u64, RaftState>) -> Result<()>;

    /// Update exhauster meta.
    async fn update_exhauster(&self, node_id: u64, endpoint: Endpoint) -> Result<()>;

    /// Random pick a available exhauster.
    async fn pick_exhauster(&self, live: Duration) -> Result<Option<u64>>;

    /// Get all responsable key ranges grouped by groups.
    async fn all_group_key_ranges(&self) -> Result<BTreeMap<u64, Vec<KeyRange>>>;

    /// Get all responsable key ranges.
    async fn all_key_ranges(&self) -> Result<Vec<KeyRange>>;

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

fn is_overlap(r1: &KeyRange, r2: &KeyRange) -> bool {
    !(r1.start_key > r2.end_key || r1.end_key < r2.start_key)
}

fn _in_range(key: &[u8], range: &KeyRange) -> bool {
    key >= &range.start_key[..] && key < &range.end_key[..]
}
