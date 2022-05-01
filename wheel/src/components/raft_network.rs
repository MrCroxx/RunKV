use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use itertools::Itertools;
use runkv_common::channel_pool::ChannelPool;
use runkv_proto::wheel::raft_service_client::RaftServiceClient;
use runkv_proto::wheel::RaftRequest;
use tokio::sync::{mpsc, RwLock};
use tonic::Request;

use crate::error::{Error, RaftManageError, Result};

#[async_trait]
pub trait RaftNetwork: Send + Sync + Clone + 'static {
    /// Register raft node info to raft network. `raft_nodes` maps raft node id to node id.
    ///
    /// Raft info must be registered first before building raft worker.
    async fn register(&self, group: u64, raft_nodes: BTreeMap<u64, u64>) -> Result<()>;

    async fn send(&self, msgs: Vec<raft::prelude::Message>) -> Result<()>;

    async fn recv(&self, msgs: Vec<raft::prelude::Message>) -> Result<()>;

    async fn take_message_rx(
        &self,
        raft_node: u64,
    ) -> Result<mpsc::UnboundedReceiver<raft::prelude::Message>>;
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

    /// Register raft node info to raft network. `raft_nodes` maps raft node id to node id.
    pub async fn register(&self, group: u64, raft_nodes: BTreeMap<u64, u64>) -> Result<()> {
        let mut guard = self.core.write().await;
        match guard.groups.entry(group) {
            Entry::Occupied(_) => return Err(RaftManageError::RaftGroupAlreadyExists(group).into()),
            Entry::Vacant(v) => {
                v.insert(raft_nodes.keys().copied().collect_vec());
            }
        }
        for (raft_node, node) in raft_nodes {
            match guard.raft_nodes.entry(raft_node) {
                Entry::Occupied(o) => {
                    // TODO: Remove inserted group.
                    return Err(RaftManageError::RaftNodeAlreadyExists {
                        group,
                        raft_node,
                        node: *o.get(),
                    }
                    .into());
                }
                Entry::Vacant(v) => {
                    v.insert(node);
                }
            }
        }
        Ok(())
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
            let (tx, rx) = mpsc::unbounded_channel();
            guard.message_channels.insert(raft_node, (tx, Some(rx)));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn send(&self, msgs: Vec<raft::prelude::Message>) -> Result<()> {
        let mut node_msgs = BTreeMap::default();
        let guard = self.core.read().await;
        for msg in msgs {
            let node =
                *guard
                    .raft_nodes
                    .get(&msg.to)
                    .ok_or(RaftManageError::RaftNodeNotExists {
                        raft_node: msg.to,
                        node: self.node,
                    })?;
            node_msgs
                .entry(node)
                .or_insert_with(|| Vec::with_capacity(10))
                .push(msg);
        }
        drop(guard);

        let futures = node_msgs
            .into_iter()
            .map(|(node, msgs)| {
                let channel_pool = self.channel_pool.clone();
                async move {
                    let channel = channel_pool.get(node).await.map_err(Error::err)?;
                    let mut client = RaftServiceClient::new(channel);
                    for msg in msgs {
                        let data = bincode::serialize(&msg).map_err(Error::serde_err)?;
                        client
                            .raft(Request::new(RaftRequest { data }))
                            .await
                            .map_err(Error::RpcStatus)?;
                    }
                    Ok::<(), Error>(())
                }
            })
            .collect_vec();
        future::try_join_all(futures).await?;

        Ok(())
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
    pub struct MockRaftNetwork(Arc<RwLock<BTreeMap<u64, MessageChannelPair>>>);

    impl Default for MockRaftNetwork {
        fn default() -> Self {
            Self(Arc::new(RwLock::new(BTreeMap::default())))
        }
    }

    #[async_trait]
    impl RaftNetwork for MockRaftNetwork {
        #[tracing::instrument(level = "trace", skip(self))]
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

        #[tracing::instrument(level = "trace", skip(self))]
        async fn send(&self, msgs: Vec<raft::prelude::Message>) -> Result<()> {
            for msg in msgs {
                self.0
                    .read()
                    .await
                    .get(&msg.to)
                    .unwrap()
                    .0
                    .send(msg)
                    .unwrap();
            }
            Ok(())
        }

        #[tracing::instrument(level = "trace", skip(self))]
        async fn recv(&self, _msgs: Vec<raft::prelude::Message>) -> Result<()> {
            unreachable!()
        }

        #[tracing::instrument(level = "trace", skip(self))]
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
