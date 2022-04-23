use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::Node;
use runkv_common::notify_pool::NotifyPool;
use runkv_proto::common::Endpoint;
use runkv_proto::kv::kv_service_server::KvService;
use runkv_proto::kv::{
    kv_op_request, kv_op_response, BytesSerde, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, KvOpRequest, PutRequest, PutResponse, TxnRequest, TxnResponse,
};
use runkv_proto::wheel::raft_service_server::RaftService;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::{
    AddEndpointsRequest, AddEndpointsResponse, AddKeyRangeRequest, AddKeyRangeResponse,
    AppendEntriesRequest, AppendEntriesResponse, InitializeRaftGroupRequest,
    InitializeRaftGroupResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use runkv_storage::raft_log_store::RaftLogStore;
use tonic::{Request, Response, Status};

use crate::components::command::Command;
use crate::components::lsm_tree::ObjectStoreLsmTree;
use crate::components::network::RaftNetwork;
use crate::components::raft_manager::RaftManager;
use crate::error::{Error, Result};
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
    pub txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
}

struct WheelInner {
    _lsm_tree: ObjectStoreLsmTree,
    meta_store: MetaStoreRef,
    channel_pool: ChannelPool,
    _raft_log_store: RaftLogStore,
    _raft_network: RaftNetwork,
    raft_manager: RaftManager,
    _txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
}

#[derive(Clone)]
pub struct Wheel {
    inner: Arc<WheelInner>,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            inner: Arc::new(WheelInner {
                _lsm_tree: options.lsm_tree,
                meta_store: options.meta_store,
                channel_pool: options.channel_pool,
                _raft_log_store: options.raft_log_store,
                _raft_network: options.raft_network,
                raft_manager: options.raft_manager,
                _txn_notify_pool: options.txn_notify_pool,
            }),
        }
    }
}

impl Wheel {
    async fn get_inner(&self, request: GetRequest) -> Result<GetResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Get(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Get(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    async fn put_inner(&self, request: PutRequest) -> Result<PutResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Put(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Put(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    async fn delete_inner(&self, request: DeleteRequest) -> Result<DeleteResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Delete(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Delete(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    async fn txn_inner(&self, request: TxnRequest) -> Result<TxnResponse> {
        let cmd = Command::TxnRequest(request);
        let _buf = cmd.encode_to_vec().map_err(Error::serde_err)?;
        // TODO: Impl me.
        // TODO: Impl me.
        // TODO: Impl me.
        Ok(TxnResponse::default())
    }
}

#[async_trait]
impl WheelService for Wheel {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn add_endpoints(
        &self,
        request: Request<AddEndpointsRequest>,
    ) -> core::result::Result<Response<AddEndpointsResponse>, Status> {
        let req = request.into_inner();
        for (node, Endpoint { host, port }) in req.endpoints.iter() {
            let node = Node {
                id: *node,
                host: host.to_owned(),
                port: *port as u16,
            };
            self.inner.channel_pool.put_node(node).await;
        }
        Ok(Response::new(AddEndpointsResponse::default()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn add_key_range(
        &self,
        request: Request<AddKeyRangeRequest>,
    ) -> core::result::Result<Response<AddKeyRangeResponse>, Status> {
        let req = request.into_inner();
        self.inner
            .meta_store
            .add_key_range(req.key_range.unwrap(), req.group, &req.raft_nodes)
            .await
            .map_err(internal)?;

        self.inner
            .raft_manager
            .update_routers(BTreeMap::from_iter(
                req.nodes
                    .iter()
                    .map(|(&raft_node, &node)| (raft_node, node)),
            ))
            .await
            .map_err(internal)?;

        for raft_node in req.raft_nodes.iter() {
            self.inner
                .raft_manager
                .add_raft_node(req.group, *raft_node)
                .await
                .map_err(internal)?;
        }

        let rsp = AddKeyRangeResponse::default();
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize_raft_group(
        &self,
        request: Request<InitializeRaftGroupRequest>,
    ) -> core::result::Result<Response<InitializeRaftGroupResponse>, Status> {
        let req = request.into_inner();
        self.inner
            .raft_manager
            .initialize_raft_group(req.leader, &req.raft_nodes)
            .await
            .map_err(internal)?;
        Ok(Response::new(InitializeRaftGroupResponse::default()))
    }
}

#[async_trait]
impl RaftService for Wheel {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> core::result::Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .inner
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> core::result::Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .inner
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn vote(
        &self,
        request: Request<VoteRequest>,
    ) -> core::result::Result<Response<VoteResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .inner
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

#[async_trait]
impl KvService for Wheel {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> core::result::Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let rsp = self.get_inner(req).await.map_err(internal)?;
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> core::result::Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let rsp = self.put_inner(req).await.map_err(internal)?;
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> core::result::Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let rsp = self.delete_inner(req).await.map_err(internal)?;
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn txn(
        &self,
        request: Request<TxnRequest>,
    ) -> core::result::Result<Response<TxnResponse>, Status> {
        let req = request.into_inner();
        let rsp = self.txn_inner(req).await.map_err(internal)?;
        Ok(Response::new(rsp))
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
        ) -> core::result::Result<Response<AppendEntriesResponse>, Status> {
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
        ) -> core::result::Result<Response<InstallSnapshotResponse>, Status> {
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
        ) -> core::result::Result<Response<VoteResponse>, Status> {
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
