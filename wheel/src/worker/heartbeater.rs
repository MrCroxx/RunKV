use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
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

pub type RaftStates = Arc<Mutex<HashMap<u64, Option<raft::SoftState>>>>;

pub struct HeartbeaterOptions {
    pub node: u64,
    pub rudder_node: u64,

    pub meta_store: MetaStoreRef,
    pub version_manager: VersionManager,
    pub channel_pool: ChannelPool,
    pub heartbeat_interval: Duration,
    pub endpoint: Endpoint,
    pub raft_states: RaftStates,
}

/// [`Heartbeater`] is respons responsible to sync local version manager.
pub struct Heartbeater {
    node: u64,
    rudder_node: u64,

    endpoint: Endpoint,
    heartbeat_interval: Duration,

    _meta_store: MetaStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,

    // TODO: Move the field into meta store.
    raft_states: RaftStates,
}

#[async_trait]
impl Worker for Heartbeater {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => warn!("error occur when heartbeater running: {}", e),
            }
        }
    }
}

impl Heartbeater {
    pub fn new(options: HeartbeaterOptions) -> Self {
        Self {
            node: options.node,
            rudder_node: options.rudder_node,

            endpoint: options.endpoint,
            heartbeat_interval: options.heartbeat_interval,

            version_manager: options.version_manager,
            _meta_store: options.meta_store,
            channel_pool: options.channel_pool,
            raft_states: options.raft_states,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        let raft_states = { self.raft_states.lock().clone() };
        let raft_states = raft_states
            .into_iter()
            .map(|(raft_node, ss)| {
                (
                    raft_node,
                    match ss {
                        Some(ss) => ss.raft_state == raft::StateRole::Leader,
                        None => false,
                    },
                )
            })
            .collect();

        let request = Request::new(HeartbeatRequest {
            node_id: self.node,
            endpoint: Some(self.endpoint.clone()),
            heartbeat_message: Some(heartbeat_request::HeartbeatMessage::WheelHeartbeat(
                WheelHeartbeatRequest {
                    watermark: self.version_manager.watermark().await,
                    next_version_id: self.version_manager.latest_version_id().await + 1,
                    raft_states,
                },
            )),
        });

        let mut client = RudderServiceClient::new(
            self.channel_pool
                .get(self.rudder_node)
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
        tokio::time::sleep(self.heartbeat_interval).await;
        Ok(())
    }
}
