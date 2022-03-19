use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::rudder::InsertL0Request;
use runkv_storage::components::{
    CachePolicy, Sstable, SstableBuilder, SstableBuilderOptions, SstableStoreRef,
};
use runkv_storage::manifest::{ManifestError, VersionManager};
use runkv_storage::utils::{timestamp, user_key, value, CompressionAlgorithm};
use tonic::transport::Channel;
use tonic::Request;
use tracing::{debug, warn};

use super::Worker;
use crate::error::Result;
use crate::storage::lsm_tree::ObjectStoreLsmTree;

pub struct SstableUploaderOptions {
    pub node_id: u64,
    pub lsm_tree: ObjectStoreLsmTree,
    pub sstable_store: SstableStoreRef,
    pub version_manager: VersionManager,
    pub sstable_capacity: usize,
    pub block_capacity: usize,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub compression_algorithm: CompressionAlgorithm,
    pub poll_interval: Duration,
    pub rudder_client: RudderServiceClient<Channel>,
}

pub struct SstableUploader {
    node_id: u64,
    options: SstableUploaderOptions,
    lsm_tree: ObjectStoreLsmTree,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
    pub rudder_client: RudderServiceClient<Channel>,
    // TODO: Get a global unique sst id from rudder.
    sstable_sequential_id: AtomicU64,
}

#[async_trait]
impl Worker for SstableUploader {
    async fn run(&mut self) -> Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => {
                    println!("error occur when uploader running: {}", e);
                    warn!("error occur when uploader running: {}", e);
                }
            }
        }
    }
}

impl SstableUploader {
    pub fn new(options: SstableUploaderOptions) -> Self {
        Self {
            node_id: options.node_id,
            lsm_tree: options.lsm_tree.clone(),
            sstable_store: options.sstable_store.clone(),
            version_manager: options.version_manager.clone(),
            rudder_client: options.rudder_client.clone(),
            options,
            sstable_sequential_id: AtomicU64::new(1),
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
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
                let mut sst_id = 0;
                while iter.valid() {
                    // TODO: Get a global unique sst id from rudder.
                    // Rotate sstable builder if necessary.
                    if sstable_builder.is_none() {
                        sst_id = self.gen_sstable_id();
                        sstable_builder =
                            Some(SstableBuilder::new(sstable_builder_options.clone()));
                        debug!("build and upload sst {}", sst_id);
                        // println!("build and upload sst {}", id);
                    }
                    if !sstable_builder.as_ref().unwrap().is_empty()
                        && sstable_builder.as_ref().unwrap().approximate_len()
                            >= self.options.sstable_capacity
                    {
                        let builder = sstable_builder.take().unwrap();
                        self.build_and_upload_sst(sst_id, builder).await?;
                        sst_ids.push(sst_id);
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
                    self.build_and_upload_sst(sst_id, builder).await?;
                    sst_ids.push(sst_id);
                }
                self.notify_update_version(sst_ids).await?;
            }

            // TODO: After local version manager awared the diff, can drop immutable table.
            // self.lsm_tree.drop_oldest_immutable_memtable();
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
            .put(&sst, data, CachePolicy::Fill)
            .await?;
        // println!("sst {} uploaded", id);
        debug!("sst {} uploaded", id);
        Ok(())
    }

    async fn notify_update_version(&mut self, sst_ids: Vec<u64>) -> Result<()> {
        // println!("notify update version");
        let request = Request::new(InsertL0Request {
            node_id: self.node_id,
            sst_ids,
            next_version_id: self.version_manager.latest_version_id().await + 1,
        });
        let rsp = self.rudder_client.insert_l0(request).await?.into_inner();
        let version_diffs = rsp.version_diffs;
        for version_diff in version_diffs {
            if let Err(runkv_storage::Error::ManifestError(ManifestError::VersionDiffIdNotMatch(
                old,
                new,
            ))) = self.version_manager.update(version_diff, true).await
            {
                warn!(
                    "version diff id not match, skip: [old: {}] [new: {}]",
                    old, new
                );
            }
        }
        self.lsm_tree.drop_oldest_immutable_memtable();
        // println!("last imm dropped");
        Ok(())
    }

    fn gen_sstable_id(&self) -> u64 {
        let sequential_id = self.sstable_sequential_id.fetch_add(1, Ordering::SeqCst);
        let node_id = self.options.node_id;
        (node_id << 32) | sequential_id
    }
}
