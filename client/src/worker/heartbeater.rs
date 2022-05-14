use std::time::Duration;

use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::Worker;
use runkv_proto::rudder::control_service_client::ControlServiceClient;
use runkv_proto::rudder::ListKeyRangesRequest;
use tonic::Request;
use tracing::warn;

use crate::error::{Error, Result};
use crate::router::Router;

pub struct HeartbeaterOptions {
    pub rudder: u64,
    pub heartbeat_interval: Duration,

    pub router: Router,
    pub channel_pool: ChannelPool,
}

pub struct Heartbeater {
    rudder: u64,
    heartbeat_interval: Duration,

    router: Router,
    channel_pool: ChannelPool,
}

impl Heartbeater {
    pub fn new(options: HeartbeaterOptions) -> Self {
        Self {
            rudder: options.rudder,
            heartbeat_interval: options.heartbeat_interval,

            router: options.router,
            channel_pool: options.channel_pool,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        loop {
            tokio::time::sleep(self.heartbeat_interval).await;
            let channel = self
                .channel_pool
                .get(self.rudder)
                .await
                .map_err(Error::err)?;
            let mut client = ControlServiceClient::new(channel);
            let infos = client
                .list_key_ranges(Request::new(ListKeyRangesRequest::default()))
                .await?
                .into_inner()
                .key_ranges;
            self.router.update_key_ranges(infos);
        }
    }
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
