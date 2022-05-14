use std::sync::Arc;
use std::time::Duration;

use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::Node;
use runkv_common::Worker;
use runkv_proto::kv::kv_service_client::KvServiceClient;
use tonic::transport::Channel;
use tracing::error;

use crate::error::Result;
use crate::router::Router;
use crate::worker::heartbeater::{Heartbeater, HeartbeaterOptions};

pub struct RunkvClientOptions {
    pub rudder: u64,
    pub rudder_host: String,
    pub rudder_port: u16,
    pub heartbeat_interval: Duration,
}

struct RunkvClientCore {
    rudder: u64,
    rudder_host: String,
    rudder_port: u16,

    channel_pool: ChannelPool,
    router: Router,
}

#[derive(Clone)]
pub struct RunkvClient {
    core: Arc<RunkvClientCore>,
}

impl RunkvClient {
    pub async fn open(options: RunkvClientOptions) -> Self {
        let router = Router::default();
        let channel_pool = ChannelPool::default();

        channel_pool
            .put_node(Node {
                id: options.rudder,
                host: options.rudder_host.clone(),
                port: options.rudder_port,
            })
            .await;

        let mut heartbeater = Heartbeater::new(HeartbeaterOptions {
            rudder: options.rudder,
            heartbeat_interval: options.heartbeat_interval,
            router: router.clone(),
            channel_pool: channel_pool.clone(),
        });
        // TODO: Keep the handle for gracefully shutdown.
        let _handle = tokio::spawn(async move {
            if let Err(e) = heartbeater.run().await {
                error!("error raised when running runkv client heartbeater: {}", e);
            }
        });

        Self {
            core: Arc::new(RunkvClientCore {
                rudder: options.rudder,
                rudder_host: options.rudder_host,
                rudder_port: options.rudder_port,
                channel_pool,
                router,
            }),
        }
    }
}
