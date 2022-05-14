use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use runkv_proto::meta::KeyRange;

use crate::error::Result;

pub mod mem;
#[allow(dead_code)]
pub mod object;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    async fn add_key_range(
        &self,
        key_range: KeyRange,
        group: u64,
        raft_nodes: &[u64],
    ) -> Result<()>;

    async fn key_ranges(&self) -> Result<Vec<KeyRange>>;

    async fn in_range(&self, key: &[u8]) -> Result<Option<(KeyRange, u64, Vec<u64>)>>;

    async fn all_in_range(&self, keys: &[&[u8]]) -> Result<Option<(KeyRange, u64, Vec<u64>)>>;

    async fn update_raft_state(
        &self,
        raft_node: u64,
        raft_state: Option<raft::SoftState>,
    ) -> Result<()>;

    async fn all_raft_states(&self) -> Result<HashMap<u64, Option<raft::SoftState>>>;

    async fn is_raft_leader(&self, raft_node: u64) -> Result<bool>;
}

pub type MetaStoreRef = Arc<dyn MetaStore>;

fn is_overlap(r1: &KeyRange, r2: &KeyRange) -> bool {
    !(r1.start_key > r2.end_key || r1.end_key < r2.start_key)
}

fn in_range(key: &[u8], range: &KeyRange) -> bool {
    key >= &range.start_key[..] && key < &range.end_key[..]
}
