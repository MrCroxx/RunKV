use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::meta::KeyRange;

use crate::error::Result;

pub mod mem;
#[allow(dead_code)]
pub mod object;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    async fn update_exhauster(&self, node_id: u64, endpoint: PbEndpoint) -> Result<()>;

    async fn pick_exhauster(&self, live: Duration) -> Result<Option<PbEndpoint>>;

    async fn update_node_ranges(&self, node_id: u64, ranges: Vec<KeyRange>) -> Result<()>;

    async fn all_node_ranges(&self) -> Result<BTreeMap<u64, Vec<KeyRange>>>;

    async fn all_ranges(&self) -> Result<Vec<KeyRange>>;
}

pub type MetaStoreRef = Arc<dyn MetaStore>;
