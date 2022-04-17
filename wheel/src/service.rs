use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_proto::wheel::raft_service_server::RaftService;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    UpdateKeyRangesRequest, UpdateKeyRangesResponse, VoteRequest, VoteResponse,
};
use runkv_storage::raft_log_store::RaftLogStore;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::components::command::AsyncCommand;
use crate::components::lsm_tree::ObjectStoreLsmTree;
use crate::components::network::RaftNetwork;
use crate::components::raft_manager::RaftManager;
use crate::error::Error;
use crate::meta::MetaStoreRef;

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct WheelOptions {
    pub lsm_tree: ObjectStoreLsmTree,
    pub meta_store: MetaStoreRef,
    pub channel_pool: ChannelPool,
    pub raft_log_store: RaftLogStore,
    pub raft_network: RaftNetwork,
    pub raft_manager: RaftManager,
    pub gear_receiver: mpsc::UnboundedReceiver<AsyncCommand>,
}

pub struct Wheel {
    _lsm_tree: ObjectStoreLsmTree,
    meta_store: MetaStoreRef,
    _channel_pool: ChannelPool,
    _raft_log_store: RaftLogStore,
    _raft_network: RaftNetwork,
    raft_manager: RaftManager,
    _gear_receiver: mpsc::UnboundedReceiver<AsyncCommand>,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            _lsm_tree: options.lsm_tree,
            meta_store: options.meta_store,
            _channel_pool: options.channel_pool,
            _raft_log_store: options.raft_log_store,
            _raft_network: options.raft_network,
            raft_manager: options.raft_manager,
            _gear_receiver: options.gear_receiver,
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
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft_manager
            .get_raft_node(req.id)
            .await
            .map_err(internal)?;
        let req_data = bincode::deserialize(&req.data)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        let rsp = raft
            .append_entries(req_data)
            .await
            .map_err(Error::raft_err)
            .map_err(internal)?;
        let rsp_data = bincode::serialize(&rsp)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        let response = AppendEntriesResponse {
            id: req.id,
            data: rsp_data,
        };
        Ok(Response::new(response))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft_manager
            .get_raft_node(req.id)
            .await
            .map_err(internal)?;
        let req_data = bincode::deserialize(&req.data)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        let rsp = raft
            .install_snapshot(req_data)
            .await
            .map_err(Error::raft_err)
            .map_err(internal)?;
        let rsp_data = bincode::serialize(&rsp)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        let response = InstallSnapshotResponse {
            id: req.id,
            data: rsp_data,
        };
        Ok(Response::new(response))
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft_manager
            .get_raft_node(req.id)
            .await
            .map_err(internal)?;
        let req_data = bincode::deserialize(&req.data)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        let rsp = raft
            .vote(req_data)
            .await
            .map_err(Error::raft_err)
            .map_err(internal)?;
        let rsp_data = bincode::serialize(&rsp)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        let response = VoteResponse {
            id: req.id,
            data: rsp_data,
        };
        Ok(Response::new(response))
    }
}

#[cfg(test)]
pub mod tests {

    use std::net::SocketAddr;

    use runkv_proto::wheel::raft_service_server::RaftServiceServer;
    use tonic::transport::Server;
    use tracing::trace;

    use super::*;

    pub struct MockRaftService {
        raft_manager: RaftManager,
    }

    impl MockRaftService {
        pub async fn bootstrap(raft_manager: RaftManager, addr: SocketAddr) {
            let service = Self { raft_manager };
            trace!("Bootstrap mock raft service on {}", addr);
            Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve(addr)
                .await
                .map_err(Error::err)
                .unwrap();
        }
    }

    #[async_trait]
    impl RaftService for MockRaftService {
        async fn append_entries(
            &self,
            request: Request<AppendEntriesRequest>,
        ) -> Result<Response<AppendEntriesResponse>, Status> {
            let req = request.into_inner();
            let raft = self
                .raft_manager
                .get_raft_node(req.id)
                .await
                .map_err(internal)?;
            let req_data = bincode::deserialize(&req.data)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            let rsp = raft
                .append_entries(req_data)
                .await
                .map_err(Error::raft_err)
                .map_err(internal)?;
            let rsp_data = bincode::serialize(&rsp)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            let response = AppendEntriesResponse {
                id: req.id,
                data: rsp_data,
            };
            Ok(Response::new(response))
        }

        async fn install_snapshot(
            &self,
            request: Request<InstallSnapshotRequest>,
        ) -> Result<Response<InstallSnapshotResponse>, Status> {
            let req = request.into_inner();
            let raft = self
                .raft_manager
                .get_raft_node(req.id)
                .await
                .map_err(internal)?;
            let req_data = bincode::deserialize(&req.data)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            let rsp = raft
                .install_snapshot(req_data)
                .await
                .map_err(Error::raft_err)
                .map_err(internal)?;
            let rsp_data = bincode::serialize(&rsp)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            let response = InstallSnapshotResponse {
                id: req.id,
                data: rsp_data,
            };
            Ok(Response::new(response))
        }

        async fn vote(
            &self,
            request: Request<VoteRequest>,
        ) -> Result<Response<VoteResponse>, Status> {
            let req = request.into_inner();
            let raft = self
                .raft_manager
                .get_raft_node(req.id)
                .await
                .map_err(internal)?;
            let req_data = bincode::deserialize(&req.data)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            let rsp = raft
                .vote(req_data)
                .await
                .map_err(Error::raft_err)
                .map_err(internal)?;
            let rsp_data = bincode::serialize(&rsp)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            let response = VoteResponse {
                id: req.id,
                data: rsp_data,
            };
            Ok(Response::new(response))
        }
    }
}
