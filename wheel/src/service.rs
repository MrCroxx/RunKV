use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_proto::wheel::raft_service_server::RaftService;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    UpdateKeyRangesRequest, UpdateKeyRangesResponse, VoteRequest, VoteResponse,
};
use runkv_storage::raft_log_store::RaftLogStore;
use tonic::{Request, Response, Status};

use crate::components::lsm_tree::ObjectStoreLsmTree;
use crate::meta::MetaStoreRef;

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct WheelOptions {
    pub lsm_tree: ObjectStoreLsmTree,
    pub meta_store: MetaStoreRef,
    pub channel_pool: ChannelPool,
    pub raft_log_store: RaftLogStore,
}

pub struct Wheel {
    _lsm_tree: ObjectStoreLsmTree,
    meta_store: MetaStoreRef,
    _channel_pool: ChannelPool,
    _raft_log_store: RaftLogStore,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            _lsm_tree: options.lsm_tree,
            meta_store: options.meta_store,
            _channel_pool: options.channel_pool,
            _raft_log_store: options.raft_log_store,
        }
    }
}

#[async_trait]
impl WheelService for Wheel {
    async fn update_key_ranges(
        &self,
        request: Request<UpdateKeyRangesRequest>,
    ) -> core::result::Result<Response<UpdateKeyRangesResponse>, Status> {
        let req = request.into_inner();
        self.meta_store
            .update_key_ranges(req.key_ranges)
            .await
            .map_err(internal)?;
        let rsp = UpdateKeyRangesResponse::default();
        Ok(Response::new(rsp))
    }
}

#[async_trait]
impl RaftService for Wheel {
    async fn append_entries(
        &self,
        _request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        todo!()
    }

    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        todo!()
    }

    async fn vote(&self, _request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        todo!()
    }
}
