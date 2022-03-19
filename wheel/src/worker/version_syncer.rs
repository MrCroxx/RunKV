use std::time::Duration;

use async_trait::async_trait;
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::rudder::HeartbeatRequest;
use runkv_storage::manifest::{ManifestError, VersionManager};
use tonic::transport::Channel;
use tonic::Request;
use tracing::warn;

use super::Worker;
use crate::error::Result;

pub struct VersionSyncerOptions {
    pub node_id: u64,
    pub version_manager: VersionManager,
    pub client: RudderServiceClient<Channel>,
    pub heartbeat_interval: Duration,
}

pub struct VersionSyncer {
    options: VersionSyncerOptions,
    version_manager: VersionManager,
    client: RudderServiceClient<Channel>,
}

#[async_trait]
impl Worker for VersionSyncer {
    async fn run(&mut self) -> Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }
}

impl VersionSyncer {
    pub fn new(options: VersionSyncerOptions) -> Self {
        Self {
            version_manager: options.version_manager.clone(),
            client: options.client.clone(),
            options,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        let request = Request::new(HeartbeatRequest {
            node_id: self.options.node_id,
            watermark: self.version_manager.watermark().await,
            next_version_id: self.version_manager.latest_version_id().await + 1,
        });
        let rsp = self.client.heartbeat(request).await?.into_inner();
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
        tokio::time::sleep(self.options.heartbeat_interval).await;
        Ok(())
    }
}
