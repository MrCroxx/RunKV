use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::coding::CompressionAlgorithm;
use runkv_common::Worker;
use runkv_proto::manifest::SstableInfo;
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::rudder::InsertL0Request;
use runkv_storage::components::{
    CachePolicy, Sstable, SstableBuilder, SstableBuilderOptions, SstableStoreRef,
};
use runkv_storage::manifest::{ManifestError, VersionManager};
use runkv_storage::utils::{timestamp, user_key, value};
use tonic::Request;
use tracing::{debug, trace, warn};

use crate::error::{Error, Result};
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
    pub channel_pool: ChannelPool,
    pub rudder_node_id: u64,
}

pub struct SstableUploader {
    node_id: u64,
    options: SstableUploaderOptions,
    lsm_tree: ObjectStoreLsmTree,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,
    rudder_node_id: u64,
    sstable_sequential_id: AtomicU64,
}

#[async_trait]
impl Worker for SstableUploader {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => {
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
            channel_pool: options.channel_pool.clone(),
            rudder_node_id: options.rudder_node_id,
            options,
            sstable_sequential_id: AtomicU64::new(1),
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        if let Some(memtable) = self.lsm_tree.get_oldest_immutable_memtable() {
            let mut sst_infos =
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
                    }
                    if !sstable_builder.as_ref().unwrap().is_empty()
                        && sstable_builder.as_ref().unwrap().approximate_len()
                            >= self.options.sstable_capacity
                    {
                        let builder = sstable_builder.take().unwrap();
                        let sst_info = self.build_and_upload_sst(sst_id, builder).await?;
                        sst_infos.push(sst_info);
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
                    let sst_info = self.build_and_upload_sst(sst_id, builder).await?;
                    sst_infos.push(sst_info);
                }
                self.notify_update_version(sst_infos).await?;
            }
        } else {
            tokio::time::sleep(self.options.poll_interval).await;
        }
        Ok(())
    }

    async fn build_and_upload_sst(&self, id: u64, builder: SstableBuilder) -> Result<SstableInfo> {
        // TODO: Async upload.
        let (meta, data) = builder.build()?;
        let data_size = meta.data_size as u64;
        let sst = Sstable::new(id, Arc::new(meta));
        trace!(
            "build sst: {}\nsmallest key: {:?}\nlargest key: {:?}",
            id,
            sst.first_key(),
            sst.last_key(),
        );
        self.sstable_store
            .put(&sst, data, CachePolicy::Fill)
            .await?;
        debug!("sst {} uploaded", id);
        Ok(SstableInfo { id, data_size })
    }

    async fn notify_update_version(&mut self, sst_infos: Vec<SstableInfo>) -> Result<()> {
        let request = Request::new(InsertL0Request {
            node_id: self.node_id,
            sst_infos,
            next_version_id: self.version_manager.latest_version_id().await + 1,
        });
        let mut client = RudderServiceClient::new(
            self.channel_pool
                .get(self.rudder_node_id)
                .await
                .map_err(Error::err)?,
        );
        let rsp = client.insert_l0(request).await?.into_inner();
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
        trace!("last imm dropped");
        Ok(())
    }

    fn gen_sstable_id(&self) -> u64 {
        let sequential_id = self.sstable_sequential_id.fetch_add(1, Ordering::SeqCst);
        let node_id = self.options.node_id;
        (node_id << 32) | sequential_id
    }
}
