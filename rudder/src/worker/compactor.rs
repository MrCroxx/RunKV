use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use runkv_common::Worker;
use runkv_storage::manifest::VersionManager;

use crate::error::Result;
use crate::meta::MetaStoreRef;

pub struct CompactorOptions {
    pub meta_store: MetaStoreRef,
    pub version_manager: VersionManager,
}

pub struct Compactor {
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
}

impl Compactor {
    pub fn new(options: CompactorOptions) -> Self {
        Self {
            version_manager: options.version_manager,
            meta_store: options.meta_store,
        }
    }

    async fn partition_points(&self) -> Result<Vec<Bytes>> {
        let ranges = self.meta_store.all_ranges().await?;
        let partition_points = ranges
            .into_iter()
            .map(|range| Bytes::from(range.start_key))
            .collect_vec();
        Ok(partition_points)
    }
}

#[async_trait]
impl Worker for Compactor {
    async fn run(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
