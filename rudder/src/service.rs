use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::Node;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::manifest::{SstableDiff, SstableOp, VersionDiff};
use runkv_proto::rudder::control_service_server::ControlService;
use runkv_proto::rudder::rudder_service_server::RudderService;
use runkv_proto::rudder::*;
use runkv_proto::wheel;
use runkv_proto::wheel::wheel_service_client::WheelServiceClient;
use runkv_storage::components::SstableStoreRef;
use runkv_storage::manifest::VersionManager;
use tonic::{Request, Response, Status};

use crate::error::Result;
use crate::meta::MetaStoreRef;

const DEFAULT_VERSION_DIFF_BATCH: usize = 10;

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct RudderOptions {
    pub node: u64,
    pub version_manager: VersionManager,
    pub sstable_store: SstableStoreRef,
    pub meta_store: MetaStoreRef,
    pub channel_pool: ChannelPool,
}

#[derive(Clone)]
pub struct Rudder {
    node: u64,
    /// Manifest of sstables.
    version_manager: VersionManager,
    channel_pool: ChannelPool,
    /// The smallest pinned sequence. Any data whose sequence is smaller than `watermark` can be
    /// safely delete.
    ///
    /// `wheel node` maintains its own watermark, and `rudder node` collects watermarks from each
    /// `wheel node` periodically and choose the min watermark among them as its own watermark.
    _watermark: Arc<AtomicU64>,
    _sstable_store: SstableStoreRef,
    meta_store: MetaStoreRef,
}

impl std::fmt::Debug for Rudder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rudder").field("node", &self.node).finish()
    }
}

impl Rudder {
    pub fn new(options: RudderOptions) -> Self {
        Self {
            node: options.node,
            version_manager: options.version_manager,
            channel_pool: options.channel_pool,
            // TODO: Restore from meta store.
            _watermark: Arc::new(AtomicU64::new(0)),
            _sstable_store: options.sstable_store,
            meta_store: options.meta_store,
        }
    }
}

impl Rudder {
    async fn handle_wheel_heartbeat(
        &self,
        _node: u64,
        _endpoint: &PbEndpoint,
        hb: WheelHeartbeatRequest,
    ) -> Result<heartbeat_response::HeartbeatMessage> {
        let version_diffs = self
            .version_manager
            .version_diffs_from(hb.next_version_id, DEFAULT_VERSION_DIFF_BATCH)
            .await?;
        self.meta_store
            .update_raft_states(hb.raft_states)
            .await
            .map_err(internal)?;
        let rsp = heartbeat_response::HeartbeatMessage::WheelHeartbeat(WheelHeartbeatResponse {
            version_diffs,
        });
        Ok(rsp)
    }

    async fn handle_exhauster_heartbeat(
        &self,
        node: u64,
        endpoint: &PbEndpoint,
        _hb: ExhausterHeartbeatRequest,
    ) -> Result<heartbeat_response::HeartbeatMessage> {
        self.channel_pool
            .put_node(Node {
                id: node,
                host: endpoint.host.clone(),
                port: endpoint.port as u16,
            })
            .await;
        self.meta_store
            .update_exhauster(node, endpoint.clone())
            .await?;
        let rsp =
            heartbeat_response::HeartbeatMessage::ExhausterHeartbeat(ExhausterHeartbeatResponse {});
        Ok(rsp)
    }
}

#[async_trait]
impl RudderService for Rudder {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> core::result::Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        let msg = match req.heartbeat_message.unwrap() {
            heartbeat_request::HeartbeatMessage::WheelHeartbeat(hb) => {
                self.handle_wheel_heartbeat(req.node_id, req.endpoint.as_ref().unwrap(), hb)
                    .await
            }
            heartbeat_request::HeartbeatMessage::ExhausterHeartbeat(hb) => {
                self.handle_exhauster_heartbeat(req.node_id, req.endpoint.as_ref().unwrap(), hb)
                    .await
            }
        }
        .map_err(internal)?;

        let rsp = HeartbeatResponse {
            heartbeat_message: Some(msg),
        };
        Ok(Response::new(rsp))
    }

    async fn insert_l0(
        &self,
        request: Request<InsertL0Request>,
    ) -> core::result::Result<Response<InsertL0Response>, Status> {
        let req = request.into_inner();
        let diff = VersionDiff {
            id: 0,
            sstable_diffs: req
                .sst_infos
                .iter()
                .map(|sst_info| SstableDiff {
                    id: sst_info.id,
                    level: 0,
                    op: SstableOp::Insert.into(),
                    data_size: sst_info.data_size,
                })
                .collect_vec(),
        };
        self.version_manager
            .update(diff, false)
            .await
            .map_err(internal)?;
        let version_diffs = self
            .version_manager
            .version_diffs_from(req.next_version_id, usize::MAX)
            .await
            .map_err(internal)?;
        let rsp = InsertL0Response { version_diffs };
        Ok(Response::new(rsp))
    }

    async fn tso(
        &self,
        _request: Request<TsoRequest>,
    ) -> core::result::Result<Response<TsoResponse>, Status> {
        let current_ts = self
            .meta_store
            .timestamp_fetch_add(1)
            .await
            .map_err(internal)?;
        let rsp = TsoResponse {
            timestamp: current_ts + 1,
        };
        Ok(Response::new(rsp))
    }
}

#[async_trait]
impl ControlService for Rudder {
    #[tracing::instrument(level = "trace")]
    async fn add_wheels(
        &self,
        request: Request<AddWheelsRequest>,
    ) -> core::result::Result<Response<AddWheelsResponse>, Status> {
        let req = request.into_inner();

        let origin_wheels = self.meta_store.wheels().await.map_err(internal)?;

        // Update meta store.
        self.meta_store
            .add_wheels(req.wheels.clone())
            .await
            .map_err(internal)?;

        // Update channel pool.
        for (&node, endpoint) in req.wheels.iter() {
            self.channel_pool
                .put_node(Node {
                    id: node,
                    host: endpoint.host.clone(),
                    port: endpoint.port as u16,
                })
                .await;
        }

        // Broadcast delta wheels to origin wheels.
        for (&node, _endpoint) in origin_wheels.iter() {
            let channel = self.channel_pool.get(node).await.map_err(internal)?;
            let mut client = WheelServiceClient::new(channel);
            let wreq = wheel::AddWheelsRequest {
                wheels: req.wheels.clone(),
            };
            client
                .add_wheels(Request::new(wreq))
                .await
                .map_err(internal)?;
        }

        // Broadcast full wheels to new wheels.
        let wheels = self.meta_store.wheels().await.map_err(internal)?;
        for (node, _endpoint) in req.wheels {
            let channel = self.channel_pool.get(node).await.map_err(internal)?;
            let mut client = WheelServiceClient::new(channel);
            let wreq = wheel::AddWheelsRequest {
                wheels: wheels.clone(),
            };
            client
                .add_wheels(Request::new(wreq))
                .await
                .map_err(internal)?;
        }

        // TODO: Broadcast full key range info to new wheels.

        let rsp = AddWheelsResponse::default();
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace")]
    async fn add_key_ranges(
        &self,
        request: Request<AddKeyRangesRequest>,
    ) -> core::result::Result<Response<AddKeyRangesResponse>, Status> {
        let req = request.into_inner();
        // Update meta store.
        self.meta_store
            .add_key_ranges(req.key_ranges.clone())
            .await
            .map_err(internal)?;

        // Broadcast requests to all wheels.
        for (node, _endpoint) in self.meta_store.wheels().await.map_err(internal)? {
            let channel = self.channel_pool.get(node).await.map_err(internal)?;
            let mut client = WheelServiceClient::new(channel);
            let wreq = wheel::AddKeyRangesRequest {
                key_ranges: req.key_ranges.clone(),
            };
            client
                .add_key_ranges(Request::new(wreq))
                .await
                .map_err(internal)?;
        }

        let rsp = AddKeyRangesResponse::default();
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace")]
    async fn router(
        &self,
        _request: Request<RouterRequest>,
    ) -> core::result::Result<Response<RouterResponse>, Status> {
        let key_ranges = self
            .meta_store
            .all_key_range_infos()
            .await
            .map_err(internal)?;
        let wheels = self.meta_store.wheels().await.map_err(internal)?;
        let rsp = RouterResponse { key_ranges, wheels };
        Ok(Response::new(rsp))
    }
}
