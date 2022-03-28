use std::time::Duration;

use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::Worker;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::rudder::{heartbeat_request, ExhausterHeartbeatRequest, HeartbeatRequest};
use tonic::Request;
use tracing::warn;

use crate::error::{err, Result};

pub struct HeartbeaterOptions {
    pub node_id: u64,
    pub endpoint: PbEndpoint,
    pub channel_pool: ChannelPool,
    pub rudder_node_id: u64,
    pub heartbeat_interval: Duration,
}

pub struct Heartbeater {
    node_id: u64,
    endpoint: PbEndpoint,
    channel_pool: ChannelPool,
    rudder_node_id: u64,
    heartbeat_interval: Duration,
}

impl Heartbeater {
    pub fn new(options: HeartbeaterOptions) -> Self {
        Self {
            node_id: options.node_id,
            endpoint: options.endpoint,
            channel_pool: options.channel_pool,
            rudder_node_id: options.rudder_node_id,
            heartbeat_interval: options.heartbeat_interval,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        tokio::time::sleep(self.heartbeat_interval).await;
        let req = HeartbeatRequest {
            node_id: self.node_id,
            endpoint: Some(self.endpoint.clone()),
            heartbeat_message: Some(heartbeat_request::HeartbeatMessage::ExhausterHeartbeat(
                ExhausterHeartbeatRequest {},
            )),
        };
        let request = Request::new(req);
        let mut client = RudderServiceClient::new(
            self.channel_pool
                .get(self.rudder_node_id)
                .await
                .map_err(err)?,
        );
        let _rsp = client.heartbeat(request).await?.into_inner();
        Ok(())
    }
}

#[async_trait]
impl Worker for Heartbeater {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => {
                    warn!("error occur when heartbeater running: {}", e);
                }
            }
        }
    }
}
