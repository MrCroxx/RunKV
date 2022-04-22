use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use runkv_common::channel_pool::ChannelPool;
use runkv_proto::wheel::raft_service_client::RaftServiceClient;
use runkv_proto::wheel::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use tonic::transport::Channel;

use super::RaftTypeConfig;
use crate::error::{Error, Result};

fn append_entries_err(
    e: Error,
) -> openraft::error::RPCError<
    RaftTypeConfig,
    openraft::error::AppendEntriesError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
}

fn install_snapshot_err(
    e: Error,
) -> openraft::error::RPCError<
    RaftTypeConfig,
    openraft::error::InstallSnapshotError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
}

fn vote_err(
    e: Error,
) -> openraft::error::RPCError<
    RaftTypeConfig,
    openraft::error::VoteError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
}

pub struct RaftNetworkConnection {
    id: u64,
    client: RaftServiceClient<Channel>,
}

#[async_trait]
impl openraft::RaftNetwork<RaftTypeConfig> for RaftNetworkConnection {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn send_append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<RaftTypeConfig>,
    ) -> core::result::Result<
        openraft::raft::AppendEntriesResponse<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            RaftTypeConfig,
            openraft::error::AppendEntriesError<
                <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
            >,
        >,
    > {
        let data = bincode::serialize(&rpc)
            .map_err(Error::serde_err)
            .map_err(append_entries_err)?;
        let request = AppendEntriesRequest { id: self.id, data };
        let response = self
            .client
            .append_entries(request)
            .await
            .map_err(Error::status)
            .map_err(append_entries_err)?;
        let resp = response.into_inner();
        Ok(bincode::deserialize(&resp.data)
            .map_err(Error::serde_err)
            .map_err(append_entries_err)?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn send_install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<RaftTypeConfig>,
    ) -> core::result::Result<
        openraft::raft::InstallSnapshotResponse<
            <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
        >,
        openraft::error::RPCError<
            RaftTypeConfig,
            openraft::error::InstallSnapshotError<
                <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
            >,
        >,
    > {
        let data = bincode::serialize(&rpc)
            .map_err(Error::serde_err)
            .map_err(install_snapshot_err)?;
        let request = InstallSnapshotRequest { id: self.id, data };
        let response = self
            .client
            .install_snapshot(request)
            .await
            .map_err(Error::status)
            .map_err(install_snapshot_err)?;
        let resp = response.into_inner();
        Ok(bincode::deserialize(&resp.data)
            .map_err(Error::serde_err)
            .map_err(install_snapshot_err)?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn send_vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<RaftTypeConfig>,
    ) -> core::result::Result<
        openraft::raft::VoteResponse<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            RaftTypeConfig,
            openraft::error::VoteError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        let data = bincode::serialize(&rpc)
            .map_err(Error::serde_err)
            .map_err(vote_err)?;
        let request = VoteRequest { id: self.id, data };
        let response = self
            .client
            .vote(request)
            .await
            .map_err(Error::status)
            .map_err(vote_err)?;
        let resp = response.into_inner();
        Ok(bincode::deserialize(&resp.data)
            .map_err(Error::serde_err)
            .map_err(vote_err)?)
    }
}

#[derive(Clone)]
pub struct RaftNetwork {
    channel_pool: ChannelPool,
    proxy: Arc<RwLock<BTreeMap<u64, u64>>>,
}

impl RaftNetwork {
    pub fn new(channel_pool: ChannelPool) -> Self {
        Self {
            channel_pool,
            proxy: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }

    pub fn update_raft_node(&self, node: u64, raft_node: u64) -> Result<()> {
        let mut guard = self.proxy.write();
        guard.insert(raft_node, node);
        Ok(())
    }
}

#[async_trait]
impl openraft::RaftNetworkFactory<RaftTypeConfig> for RaftNetwork {
    type Network = RaftNetworkConnection;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn connect(
        &mut self,
        target: <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
        _node: Option<&openraft::Node>,
    ) -> Self::Network {
        let raft_node = target;
        let node = *self
            .proxy
            .read()
            .get(&raft_node)
            .unwrap_or_else(|| panic!("proxy to {} not found", raft_node));
        let client = RaftServiceClient::new(self.channel_pool.get(node).await.unwrap());
        RaftNetworkConnection {
            id: raft_node,
            client,
        }
    }
}
