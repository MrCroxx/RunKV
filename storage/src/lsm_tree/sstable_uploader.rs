use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use runkv_proto::manifest::{SsTableDiff, SsTableOp};
use tracing::{debug, warn};

use crate::components::{Sstable, SstableBuilder, SstableBuilderOptions, SstableStoreRef};
use crate::manifest::VersionManager;
use crate::object_store_lsm_tree::ObjectStoreLsmTree;
use crate::utils::{timestamp, user_key, value, CompressionAlgorithm};
use crate::Result;

pub struct SstableUploaderOptions {
    pub lsm_tree: ObjectStoreLsmTree,
    pub sstable_store: SstableStoreRef,
    pub version_manager: VersionManager,
    pub sstable_capacity: usize,
    pub block_capacity: usize,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub compression_algorithm: CompressionAlgorithm,
    pub poll_interval: Duration,
}

pub struct SstableUploader {
    options: SstableUploaderOptions,
    lsm_tree: ObjectStoreLsmTree,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
    // TODO: Get a global unique sst id from rudder.
    id: AtomicU64,
}

impl SstableUploader {
    pub fn new(options: SstableUploaderOptions) -> Self {
        Self {
            lsm_tree: options.lsm_tree.clone(),
            sstable_store: options.sstable_store.clone(),
            version_manager: options.version_manager.clone(),
            options,
            id: AtomicU64::new(1),
        }
    }

    pub async fn run(&self) -> Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }

    async fn run_inner(&self) -> Result<()> {
        if let Some(memtable) = self.lsm_tree.get_oldest_immutable_memtable() {
            let mut sst_ids =
                Vec::with_capacity(memtable.mem_size() / self.options.sstable_capacity + 1);
            if !memtable.is_empty() {
                let sstable_builder_options = SstableBuilderOptions {
                    capacity: self.options.sstable_capacity,
                    block_capacity: self.options.block_capacity,
                    restart_interval: self.options.restart_interval,
                    bloom_false_positive: self.options.bloom_false_positive,
                    compression_algorithm: self.options.compression_algorithm,
                };
                let mut sstable_builder = None;
                let skiplist = memtable.unwrap();
                let mut iter = skiplist.iter();
                iter.seek_to_first();
                let mut id = 0;
                while iter.valid() {
                    // TODO: Get a global unique sst id from rudder.
                    id = self.id.load(Ordering::SeqCst);
                    // Rotate sstable builder if necessary.
                    if sstable_builder.is_none() {
                        sstable_builder =
                            Some(SstableBuilder::new(sstable_builder_options.clone()));
                        debug!("build and upload sst {}", id);
                        // println!("build and upload sst {}", id);
                    }
                    if !sstable_builder.as_ref().unwrap().is_empty()
                        && sstable_builder.as_ref().unwrap().approximate_len()
                            >= self.options.sstable_capacity
                    {
                        let builder = sstable_builder.take().unwrap();
                        self.build_and_upload_sst(id, builder).await?;
                        sst_ids.push(id);
                        continue;
                    }

                    // Fill sst.
                    let builder = sstable_builder.as_mut().unwrap();
                    let fk = iter.key();
                    let uk = user_key(fk);
                    let ts = timestamp(fk);
                    let vraw = iter.value();
                    builder.add(uk, ts, value(vraw))?;
                    iter.next();
                }
                if let Some(builder) = sstable_builder.take() {
                    self.build_and_upload_sst(id, builder).await?;
                    sst_ids.push(id);
                }
                self.notify_update_version(sst_ids).await?;
            }

            // TODO: After local version manager awared the diff, can drop immutable table.
            self.lsm_tree.drop_oldest_immutable_memtable();
        } else {
            tokio::time::sleep(self.options.poll_interval).await;
        }
        Ok(())
    }

    async fn build_and_upload_sst(&self, id: u64, builder: SstableBuilder) -> Result<()> {
        // TODO: Async upload.
        let (meta, data) = builder.build()?;
        let sst = Sstable::new(id, Arc::new(meta));

        self.sstable_store
            .put(&sst, data, crate::components::CachePolicy::Fill)
            .await?;
        // println!("sst {} uploaded", id);
        debug!("sst {} uploaded", id);
        self.id.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn notify_update_version(&self, sst_ids: Vec<u64>) -> Result<()> {
        // TODO: Call global version manager, let it update local version manager.
        self.version_manager
            .update(
                runkv_proto::manifest::VersionDiff {
                    id: 0,
                    sstable_diffs: sst_ids
                        .into_iter()
                        .map(|sst_id| SsTableDiff {
                            id: sst_id,
                            level: 0,
                            op: SsTableOp::Insert.into(),
                        })
                        .collect_vec(),
                },
                false,
            )
            .await?;
        Ok(())
    }
}

pub type SstableUploaderRef = Arc<SstableUploader>;
