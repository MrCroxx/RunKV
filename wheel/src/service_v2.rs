use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::Node;
use runkv_common::notify_pool::NotifyPool;
use runkv_proto::common::Endpoint;
use runkv_proto::kv::kv_service_server::KvService;
use runkv_proto::kv::{
    kv_op_request, kv_op_response, BytesSerde, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, KvOpRequest, PutRequest, PutResponse, SnapshotRequest, SnapshotResponse,
    TxnRequest, TxnResponse,
};
use runkv_proto::wheel::raft_service_server::RaftService;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::{
    AddEndpointsRequest, AddEndpointsResponse, AddKeyRangeRequest, AddKeyRangeResponse,
    AppendEntriesRequest, AppendEntriesResponse, InitializeRaftGroupRequest,
    InitializeRaftGroupResponse, InstallSnapshotRequest, InstallSnapshotResponse, RaftRequest,
    RaftResponse, VoteRequest, VoteResponse,
};
use tonic::{Request, Response, Status};
use tracing::{trace_span, Instrument};

use crate::components::command::Command;
use crate::components::raft_manager_v2::RaftManager;
use crate::components::raft_network::{GrpcRaftNetwork, RaftNetwork};
use crate::error::{Error, KvError, Result};
use crate::meta::MetaStoreRef;
use crate::worker::raft::Proposal;

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct WheelOptions {
    pub meta_store: MetaStoreRef,
    pub channel_pool: ChannelPool,
    pub raft_network: GrpcRaftNetwork,
    pub raft_manager: RaftManager,
    pub txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
}

struct WheelInner {
    meta_store: MetaStoreRef,
    channel_pool: ChannelPool,
    raft_network: GrpcRaftNetwork,
    raft_manager: RaftManager,
    txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    request_id: AtomicU64,
}

#[derive(Clone)]
pub struct Wheel {
    inner: Arc<WheelInner>,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            inner: Arc::new(WheelInner {
                meta_store: options.meta_store,
                channel_pool: options.channel_pool,
                raft_network: options.raft_network,
                raft_manager: options.raft_manager,
                txn_notify_pool: options.txn_notify_pool,
                request_id: AtomicU64::new(0),
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

    async fn snapshot_inner(&self, request: SnapshotRequest) -> Result<SnapshotResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Snapshot(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Snapshot(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn txn_inner(&self, request: TxnRequest) -> Result<TxnResponse> {
        // Pick raft leader of the request.
        let raft_nodes = self.txn_raft_nodes(&request).await?;
        assert!(!raft_nodes.is_empty());
        let raft_node = *raft_nodes.first().unwrap();

        let read_only = request.ops.iter().all(|op| {
            matches!(
                op.request,
                Some(kv_op_request::Request::Snapshot(_)) | Some(kv_op_request::Request::Get(_))
            )
        });
        let sequence = self.inner.raft_manager.get_sequence(raft_node).await?;
        let sequence = if read_only {
            sequence.load(Ordering::Acquire)
        } else {
            sequence.fetch_add(1, Ordering::SeqCst) + 1
        };

        // Register request.
        let id = self.inner.request_id.fetch_add(1, Ordering::SeqCst) + 1;
        let rx = self
            .inner
            .txn_notify_pool
            .register(id)
            .map_err(Error::err)?;

        // Propose cmd with raft leader.
        let cmd = Command::TxnRequest {
            request_id: id,
            sequence,
            request,
        };
        let buf = cmd.encode_to_vec().map_err(Error::serde_err)?;

        let proposal_tx = self
            .inner
            .raft_manager
            .get_proposal_channel(raft_node)
            .await?;

        proposal_tx
            .send(Proposal {
                data: buf,
                context: vec![],
            })
            .map_err(Error::err)?;

        // Wait for resposne.
        let response = rx
            .instrument(trace_span!("wait_apply"))
            .await
            .map_err(Error::err)?;
        response
    }

    async fn txn_raft_nodes<'a>(&self, request: &'a TxnRequest) -> Result<Vec<u64>> {
        assert!(!request.ops.is_empty());

        let key = |req: &'a kv_op_request::Request| -> &'a [u8] {
            match req {
                kv_op_request::Request::Get(GetRequest { key, .. }) => key,
                kv_op_request::Request::Put(PutRequest { key, .. }) => key,
                kv_op_request::Request::Delete(DeleteRequest { key }) => key,
                kv_op_request::Request::Snapshot(SnapshotRequest { key }) => key,
            }
        };

        let keys = request
            .ops
            .iter()
            .map(|op| key(op.request.as_ref().unwrap()))
            .collect_vec();

        let (_range, _group, raft_nodes) = self
            .inner
            .meta_store
            .all_in_range(&keys)
            .await?
            .ok_or_else(|| KvError::InvalidShard(format!("request {:?}", request)))?;

        // TODO: Find the potential leader.
        Ok(raft_nodes)
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

        for raft_node in req.raft_nodes.iter() {
            self.inner
                .raft_manager
                .create_raft_node(req.group, *raft_node)
                .await
                .map_err(internal)?;
        }

        let rsp = AddKeyRangeResponse::default();
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize_raft_group(
        &self,
        _request: Request<InitializeRaftGroupRequest>,
    ) -> core::result::Result<Response<InitializeRaftGroupResponse>, Status> {
        unreachable!()
    }
}

#[async_trait]
impl RaftService for Wheel {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn append_entries(
        &self,
        _request: Request<AppendEntriesRequest>,
    ) -> core::result::Result<Response<AppendEntriesResponse>, Status> {
        unreachable!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotRequest>,
    ) -> core::result::Result<Response<InstallSnapshotResponse>, Status> {
        unreachable!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn vote(
        &self,
        _request: Request<VoteRequest>,
    ) -> core::result::Result<Response<VoteResponse>, Status> {
        unreachable!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn raft(
        &self,
        request: Request<RaftRequest>,
    ) -> core::result::Result<Response<RaftResponse>, Status> {
        let req = request.into_inner();
        let msg = bincode::deserialize(&req.data)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        self.inner
            .raft_network
            .recv(vec![msg])
            .await
            .map_err(internal)?;
        let rsp = RaftResponse::default();
        Ok(Response::new(rsp))
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
    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> core::result::Result<Response<SnapshotResponse>, Status> {
        let req = request.into_inner();
        let rsp = self.snapshot_inner(req).await.map_err(internal)?;
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
