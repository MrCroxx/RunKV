use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use runkv_common::channel_pool::ChannelPool;
use runkv_proto::wheel::raft_service_client::RaftServiceClient;
use runkv_proto::wheel::RaftRequest;
use tokio::sync::{mpsc, RwLock};
use tonic::transport::Channel;
use tonic::Request;

use crate::error::{Error, RaftManageError, Result};

#[async_trait]
pub trait RaftNetwork: Send + Sync + Clone + 'static {
    type RaftClient: RaftClient;

    /// Register raft node info to raft network. `raft_nodes` maps raft node id to node id.
    ///
    /// Raft info must be registered first before building raft worker.
    async fn register(&self, group: u64, raft_nodes: BTreeMap<u64, u64>) -> Result<()>;

    async fn client(&self, raft_node: u64) -> Result<Self::RaftClient>;

    async fn recv(&self, msgs: Vec<raft::prelude::Message>) -> Result<()>;

    async fn take_message_rx(
        &self,
        raft_node: u64,
    ) -> Result<mpsc::UnboundedReceiver<raft::prelude::Message>>;
}

#[async_trait]
pub trait RaftClient: Send + Sync + Clone + 'static {
    async fn send(&mut self, msgs: Vec<raft::prelude::Message>) -> Result<()>;
}

#[derive(Clone)]
pub struct GrpcRaftClient {
    client: RaftServiceClient<Channel>,
}

impl GrpcRaftClient {
    pub fn new(client: RaftServiceClient<Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RaftClient for GrpcRaftClient {
    async fn send(&mut self, msgs: Vec<raft::prelude::Message>) -> Result<()> {
        let data = bincode::serialize(&msgs).map_err(Error::serde_err)?;
        let req = RaftRequest { data };
        self.client
            .raft(Request::new(req))
            .await
            .map_err(Error::RpcStatus)?;
        Ok(())
    }
}

type MessageChannelPair = (
    mpsc::UnboundedSender<raft::prelude::Message>,
    Option<mpsc::UnboundedReceiver<raft::prelude::Message>>,
);

struct GrpcRaftNetworkCore {
    /// `{ raft node -> node }`
    raft_nodes: BTreeMap<u64, u64>,
    /// `{ raft node -> channels }`
    message_channels: BTreeMap<u64, MessageChannelPair>,
    /// `{ group -> [ raft node, .. ] }`
    groups: BTreeMap<u64, Vec<u64>>,
}

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    node: u64,
    core: Arc<RwLock<GrpcRaftNetworkCore>>,
    channel_pool: ChannelPool,
}

impl GrpcRaftNetwork {
    pub fn new(node: u64, channel_pool: ChannelPool) -> Self {
        Self {
            node,
            core: Arc::new(RwLock::new(GrpcRaftNetworkCore {
                raft_nodes: BTreeMap::default(),
                message_channels: BTreeMap::default(),
                groups: BTreeMap::default(),
            })),
            channel_pool,
        }
    }

    pub async fn raft_nodes(&self, group: u64) -> Result<Vec<u64>> {
        let guard = self.core.read().await;
        let raft_nodes = guard
            .groups
            .get(&group)
            .ok_or(RaftManageError::RaftGroupNotExists(group))?;
        let raft_nodes = raft_nodes.iter().copied().collect_vec();
        Ok(raft_nodes)
    }
}

#[async_trait]
impl RaftNetwork for GrpcRaftNetwork {
    type RaftClient = GrpcRaftClient;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn register(&self, group: u64, raft_nodes: BTreeMap<u64, u64>) -> Result<()> {
        let mut guard = self.core.write().await;
        match guard.groups.entry(group) {
            Entry::Occupied(_) => return Err(RaftManageError::RaftGroupAlreadyExists(group).into()),
            Entry::Vacant(v) => {
                v.insert(raft_nodes.keys().copied().collect_vec());
            }
        }
        for (raft_node, node) in raft_nodes {
            if guard.raft_nodes.get(&raft_node).is_some() {
                guard.groups.remove(&group);
                return Err(RaftManageError::RaftNodeAlreadyExists {
                    group,
                    raft_node,
                    node,
                }
                .into());
            }
            guard.raft_nodes.insert(raft_node, node);
            // FIXME: Chnanels doesn't belong to the current node should not be created.
            let (tx, rx) = mpsc::unbounded_channel();
            guard.message_channels.insert(raft_node, (tx, Some(rx)));
        }
        Ok(())
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn client(&self, raft_node: u64) -> Result<GrpcRaftClient> {
        let guard = self.core.read().await;
        let node = *guard
            .raft_nodes
            .get(&raft_node)
            .ok_or(RaftManageError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            })?;
        let channel = self.channel_pool.get(node).await.map_err(Error::err)?;
        let client = RaftServiceClient::new(channel);
        let client = GrpcRaftClient { client };
        Ok(client)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn recv(&self, msgs: Vec<raft::prelude::Message>) -> Result<()> {
        let guard = self.core.read().await;
        for msg in msgs {
            let tx = &guard
                .message_channels
                .get(&msg.to)
                .ok_or(RaftManageError::RaftNodeNotExists {
                    raft_node: msg.to,
                    node: self.node,
                })?
                .0;
            tx.send(msg).map_err(Error::err)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn take_message_rx(
        &self,
        raft_node: u64,
    ) -> Result<mpsc::UnboundedReceiver<raft::prelude::Message>> {
        let mut guard = self.core.write().await;
        let channel = guard.message_channels.get_mut(&raft_node).ok_or(
            RaftManageError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            },
        )?;
        let rx = channel.1.take().ok_or_else(|| {
            RaftManageError::Other(format!(
                "message rx of raft node {} has already been taken",
                raft_node
            ))
        })?;
        Ok(rx)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[derive(Clone)]
    pub struct MockRaftClient(mpsc::UnboundedSender<raft::prelude::Message>);

    #[async_trait]
    impl RaftClient for MockRaftClient {
        async fn send(&mut self, msgs: Vec<raft::prelude::Message>) -> Result<()> {
            for msg in msgs {
                self.0.send(msg).unwrap();
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct MockRaftNetwork(Arc<RwLock<BTreeMap<u64, MessageChannelPair>>>);

    impl Default for MockRaftNetwork {
        fn default() -> Self {
            Self(Arc::new(RwLock::new(BTreeMap::default())))
        }
    }

    #[async_trait]
    impl RaftNetwork for MockRaftNetwork {
        type RaftClient = MockRaftClient;

        async fn register(&self, _group: u64, raft_nodes: BTreeMap<u64, u64>) -> Result<()> {
            let mut guard = self.0.write().await;
            for (raft_node, _) in raft_nodes {
                let (tx, rx) = mpsc::unbounded_channel();
                if guard.insert(raft_node, (tx, Some(rx))).is_some() {
                    panic!("redundant raft node");
                };
            }
            Ok(())
        }

        async fn client(&self, raft_node: u64) -> Result<MockRaftClient> {
            let tx = self.0.read().await.get(&raft_node).unwrap().0.clone();
            Ok(MockRaftClient(tx))
        }

        async fn recv(&self, _msgs: Vec<raft::prelude::Message>) -> Result<()> {
            unreachable!()
        }

        async fn take_message_rx(
            &self,
            raft_node: u64,
        ) -> Result<mpsc::UnboundedReceiver<raft::prelude::Message>> {
            Ok(self
                .0
                .write()
                .await
                .get_mut(&raft_node)
                .unwrap()
                .1
                .take()
                .unwrap())
        }
    }
}
