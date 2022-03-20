use std::time::Duration;

use async_trait::async_trait;
use runkv_common::Worker;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::rudder::{heartbeat_request, ExhausterHeartbeatRequest, HeartbeatRequest};
use tonic::transport::Channel;
use tonic::Request;
use tracing::warn;

use crate::error::Result;

pub struct HeartbeaterOptions {
    pub node_id: u64,
    pub endpoint: PbEndpoint,
    pub client: RudderServiceClient<Channel>,
    pub heartbeat_interval: Duration,
}

pub struct Heartbeater {
    node_id: u64,
    endpoint: PbEndpoint,
    client: RudderServiceClient<Channel>,
    heartbeat_interval: Duration,
}

impl Heartbeater {
    pub fn new(options: HeartbeaterOptions) -> Self {
        Self {
            node_id: options.node_id,
            endpoint: options.endpoint,
            client: options.client,
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
        let _rsp = self.client.heartbeat(request).await?.into_inner();
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
                    println!("error occur when uploader running: {}", e);
                    warn!("error occur when uploader running: {}", e);
                }
            }
        }
    }
}
