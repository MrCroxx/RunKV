use async_trait::async_trait;
use itertools::Itertools;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::manifest::{SsTableDiff, SsTableOp, VersionDiff};
use runkv_proto::rudder::rudder_service_server::RudderService;
use runkv_proto::rudder::{
    heartbeat_request, heartbeat_response, ExhausterHeartbeatRequest, ExhausterHeartbeatResponse,
    HeartbeatRequest, HeartbeatResponse, InsertL0Request, InsertL0Response, WheelHeartbeatRequest,
    WheelHeartbeatResponse,
};
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
    pub version_manager: VersionManager,
    pub sstable_store: SstableStoreRef,
    pub meta_store: MetaStoreRef,
}

pub struct Rudder {
    /// Manifest of sstables.
    version_manager: VersionManager,
    /// The smallest pinned timestamp. Any data whose timestamp is smaller than `watermark` can be
    /// safely delete.
    ///
    /// `wheel node` maintains its own watermark, and `rudder node` collects watermarks from each
    /// `wheel node` periodically and choose the min watermark among them as its own watermark.
    _watermark: u64,
    _sstable_store: SstableStoreRef,
    meta_store: MetaStoreRef,
}

impl Rudder {
    pub fn new(options: RudderOptions) -> Self {
        Self {
            version_manager: options.version_manager,
            // TODO: Restore from meta store.
            _watermark: 0,
            _sstable_store: options.sstable_store,
            meta_store: options.meta_store,
        }
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
                .map(|sst_info| SsTableDiff {
                    id: sst_info.id,
                    level: 0,
                    op: SsTableOp::Insert.into(),
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
}

impl Rudder {
    async fn handle_wheel_heartbeat(
        &self,
        node_id: u64,
        _endpoint: &PbEndpoint,
        hb: WheelHeartbeatRequest,
    ) -> Result<heartbeat_response::HeartbeatMessage> {
        let version_diffs = self
            .version_manager
            .version_diffs_from(hb.next_version_id, DEFAULT_VERSION_DIFF_BATCH)
            .await?;
        self.meta_store
            .update_node_ranges(node_id, hb.key_ranges)
            .await?;
        let rsp = heartbeat_response::HeartbeatMessage::WheelHeartbeat(WheelHeartbeatResponse {
            version_diffs,
        });
        Ok(rsp)
    }

    async fn handle_exhauster_heartbeat(
        &self,
        node_id: u64,
        endpoint: &PbEndpoint,
        _hb: ExhausterHeartbeatRequest,
    ) -> Result<heartbeat_response::HeartbeatMessage> {
        self.meta_store
            .update_exhauster(node_id, endpoint.clone())
            .await?;
        let rsp =
            heartbeat_response::HeartbeatMessage::ExhausterHeartbeat(ExhausterHeartbeatResponse {});
        Ok(rsp)
    }
}
