use std::sync::Arc;
use std::time::Duration;

use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::Node;
use runkv_common::Worker;
use runkv_proto::kv::kv_service_client::KvServiceClient;
use runkv_proto::kv::*;
use runkv_proto::rudder::control_service_client::ControlServiceClient;
use runkv_proto::rudder::RouterRequest;
use tonic::Request;
use tracing::error;

use crate::error::{Error, KvError, Result};
use crate::router::Router;
use crate::worker::heartbeater::{Heartbeater, HeartbeaterOptions};

pub struct RunkvClientOptions {
    pub rudder: u64,
    pub rudder_host: String,
    pub rudder_port: u16,
    /// Duration to auto sync router map. Disable auto sync if duration is zero.
    pub heartbeat_interval: Duration,
}

struct RunkvClientCore {
    channel_pool: ChannelPool,
    router: Router,
}

#[derive(Clone)]
pub struct RunkvClient {
    rudder: u64,
    _rudder_host: String,
    _rudder_port: u16,

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

        if !options.heartbeat_interval.is_zero() {
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
        }

        Self {
            rudder: options.rudder,
            _rudder_host: options.rudder_host,
            _rudder_port: options.rudder_port,

            core: Arc::new(RunkvClientCore {
                channel_pool,
                router,
            }),
        }
    }

    pub async fn update_router(&self) -> Result<()> {
        let channel = self
            .core
            .channel_pool
            .get(self.rudder)
            .await
            .map_err(Error::err)?;
        let mut client = ControlServiceClient::new(channel);
        let rsp = client
            .router(Request::new(RouterRequest::default()))
            .await?
            .into_inner();
        self.core.router.update_key_ranges(rsp.key_ranges);
        for (node, endpoint) in rsp.wheels {
            self.core
                .channel_pool
                .put_node(Node {
                    id: node,
                    host: endpoint.host,
                    port: endpoint.port as u16,
                })
                .await;
        }
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let leader = match self.core.router.leader(&key) {
            Some(leader) => leader,
            None => return Err(KvError::TemporarilyNoLeader(key).into()),
        };
        let channel = self
            .core
            .channel_pool
            .get(leader.node)
            .await
            .map_err(Error::err)?;
        let mut client = KvServiceClient::new(channel);
        let mut rsp = client
            .kv(Request::new(KvRequest {
                ops: vec![Op {
                    r#type: OpType::Get.into(),
                    key,
                    ..Default::default()
                }],
                target: leader.raft_node,
            }))
            .await?
            .into_inner();
        if rsp.err() == ErrCode::Redirect {
            return Err(KvError::Redirect.into());
        }
        let raw = rsp.ops.remove(0).value;
        let value = if raw.is_empty() { None } else { Some(raw) };
        Ok(value)
    }

    /// Snapshot get.
    pub async fn sget(&self, key: Vec<u8>, sequence: u64) -> Result<Option<Vec<u8>>> {
        let leader = match self.core.router.leader(&key) {
            Some(leader) => leader,
            None => return Err(KvError::TemporarilyNoLeader(key).into()),
        };
        let channel = self
            .core
            .channel_pool
            .get(leader.node)
            .await
            .map_err(Error::err)?;
        let mut client = KvServiceClient::new(channel);
        let mut rsp = client
            .kv(Request::new(KvRequest {
                ops: vec![Op {
                    r#type: OpType::Get.into(),
                    key,
                    sequence,
                    ..Default::default()
                }],
                target: leader.raft_node,
            }))
            .await?
            .into_inner();
        if rsp.err() == ErrCode::Redirect {
            return Err(KvError::Redirect.into());
        }
        let raw = rsp.ops.remove(0).value;
        let value = if raw.is_empty() { None } else { Some(raw) };
        Ok(value)
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        assert!(!value.is_empty());
        let leader = match self.core.router.leader(&key) {
            Some(leader) => leader,
            None => return Err(KvError::TemporarilyNoLeader(key).into()),
        };
        let channel = self
            .core
            .channel_pool
            .get(leader.node)
            .await
            .map_err(Error::err)?;
        let mut client = KvServiceClient::new(channel);
        let rsp = client
            .kv(Request::new(KvRequest {
                ops: vec![Op {
                    r#type: OpType::Put.into(),
                    key,
                    value,
                    ..Default::default()
                }],
                target: leader.raft_node,
            }))
            .await?
            .into_inner();
        if rsp.err() == ErrCode::Redirect {
            return Err(KvError::Redirect.into());
        }
        Ok(())
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        let leader = match self.core.router.leader(&key) {
            Some(leader) => leader,
            None => return Err(KvError::TemporarilyNoLeader(key).into()),
        };
        let channel = self
            .core
            .channel_pool
            .get(leader.node)
            .await
            .map_err(Error::err)?;
        let mut client = KvServiceClient::new(channel);
        let rsp = client
            .kv(Request::new(KvRequest {
                ops: vec![Op {
                    r#type: OpType::Delete.into(),
                    key,
                    ..Default::default()
                }],
                target: leader.raft_node,
            }))
            .await?
            .into_inner();
        if rsp.err() == ErrCode::Redirect {
            return Err(KvError::Redirect.into());
        }
        Ok(())
    }
}
