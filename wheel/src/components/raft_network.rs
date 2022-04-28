use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::Arc;

use itertools::Itertools;
use runkv_common::channel_pool::ChannelPool;
use runkv_proto::wheel::raft_service_client::RaftServiceClient;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::error::{Error, RaftManageError, Result};

struct RaftNetworkCore {
    /// `{ raft node -> node }`
    raft_nodes: BTreeMap<u64, u64>,
    /// `{ group -> [raft node, ..] }`
    groups: BTreeMap<u64, Vec<u64>>,
}

#[derive(Clone)]
pub struct RaftNetwork {
    node: u64,
    core: Arc<RwLock<RaftNetworkCore>>,
    channel_pool: ChannelPool,
}

impl RaftNetwork {
    pub fn new(node: u64, channel_pool: ChannelPool) -> Self {
        Self {
            node,
            core: Arc::new(RwLock::new(RaftNetworkCore {
                raft_nodes: BTreeMap::default(),
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

    pub async fn client(&self, raft_node: u64) -> Result<RaftServiceClient<Channel>> {
        let guard = self.core.read().await;
        let node = *guard
            .raft_nodes
            .get(&raft_node)
            .ok_or(RaftManageError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            })?;
        let channel = self.channel_pool.get(node).await.map_err(Error::err)?;
        Ok(RaftServiceClient::new(channel))
    }
}
