use std::collections::BTreeSet;

use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use runkv_proto::meta::KeyRange;

use super::MetaStore;
use crate::error::Result;

#[derive(Default)]
struct MemoryMetaStoreCore {
    key_ranges: BTreeSet<KeyRange>,
}

#[derive(Default)]
pub struct MemoryMetaStore {
    inner: RwLock<MemoryMetaStoreCore>,
}

impl MemoryMetaStore {}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn update_key_ranges(&self, key_ranges: Vec<KeyRange>) -> Result<()> {
        let mut guard = self.inner.write();
        guard.key_ranges.clear();
        guard.key_ranges.extend(key_ranges.into_iter());
        Ok(())
    }

    async fn key_ranges(&self) -> Result<Vec<KeyRange>> {
        let guard = self.inner.read();
        Ok(guard.key_ranges.iter().cloned().collect_vec())
    }

    async fn in_ranges(&self, key: &[u8]) -> Result<bool> {
        let guard = self.inner.read();
        for key_range in guard.key_ranges.iter() {
            if &key_range.start_key[..] > key {
                return Ok(false);
            }
            if &key_range.start_key[..] <= key && key <= &key_range.end_key[..] {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
