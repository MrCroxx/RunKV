use std::time::Duration;

use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::Worker;
use runkv_proto::common::Endpoint;
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::rudder::{
    heartbeat_request, heartbeat_response, HeartbeatRequest, WheelHeartbeatRequest,
};
use runkv_storage::manifest::{ManifestError, VersionManager};
use tonic::Request;
use tracing::warn;

use crate::error::{Error, Result};
use crate::meta::MetaStoreRef;

pub struct HeartbeaterOptions {
    pub node_id: u64,
    pub meta_store: MetaStoreRef,
    pub version_manager: VersionManager,
    pub channel_pool: ChannelPool,
    pub rudder_node_id: u64,
    pub heartbeat_interval: Duration,
    pub endpoint: Endpoint,
}

/// [`Heartbeater`] is respons responsible to sync local version manager.
pub struct Heartbeater {
    options: HeartbeaterOptions,
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,
    rudder_node_id: u64,
}

#[async_trait]
impl Worker for Heartbeater {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }
}

impl Heartbeater {
    pub fn new(options: HeartbeaterOptions) -> Self {
        Self {
            version_manager: options.version_manager.clone(),
            meta_store: options.meta_store.clone(),
            channel_pool: options.channel_pool.clone(),
            rudder_node_id: options.rudder_node_id,
            options,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        let request = Request::new(HeartbeatRequest {
            node_id: self.options.node_id,
            endpoint: Some(self.options.endpoint.clone()),
            heartbeat_message: Some(heartbeat_request::HeartbeatMessage::WheelHeartbeat(
                WheelHeartbeatRequest {
                    watermark: self.version_manager.watermark().await,
                    next_version_id: self.version_manager.latest_version_id().await + 1,
                    key_ranges: self.meta_store.key_ranges().await?,
                },
            )),
        });
        let mut client = RudderServiceClient::new(
            self.channel_pool
                .get(self.rudder_node_id)
                .await
                .map_err(Error::err)?,
        );
        let rsp = client.heartbeat(request).await?.into_inner();

        let hb = match rsp.heartbeat_message.unwrap() {
            heartbeat_response::HeartbeatMessage::WheelHeartbeat(hb) => hb,
            _ => unreachable!(),
        };
        for version_diff in hb.version_diffs {
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
