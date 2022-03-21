use std::sync::Arc;

use async_trait::async_trait;
use runkv_proto::meta::KeyRange;

use crate::error::Result;

pub mod mem;
#[allow(dead_code)]
pub mod object;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    async fn update_key_ranges(&self, key_ranges: Vec<KeyRange>) -> Result<()>;

    async fn key_ranges(&self) -> Result<Vec<KeyRange>>;

    async fn in_ranges(&self, key: &[u8]) -> Result<bool>;
}

pub type MetaStoreRef = Arc<dyn MetaStore>;
