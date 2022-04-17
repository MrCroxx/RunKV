use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use crate::config::Node;

struct ChannelPoolCore {
    endpoints: BTreeMap<u64, Endpoint>,
    channels: BTreeMap<u64, Channel>,
}

#[derive(Clone)]
pub struct ChannelPool {
    core: Arc<Mutex<ChannelPoolCore>>,
}

fn endpoint(node: &Node) -> Endpoint {
    Endpoint::from_shared(format!("http://{}:{}", node.host, node.port)).unwrap()
}

impl Default for ChannelPool {
    fn default() -> Self {
        Self::with_nodes(vec![])
    }
}

impl ChannelPool {
    pub fn with_nodes(nodes: Vec<Node>) -> Self {
        Self {
            core: Arc::new(Mutex::new(ChannelPoolCore {
                endpoints: BTreeMap::from_iter(
                    nodes.into_iter().map(|node| (node.id, endpoint(&node))),
                ),
                channels: BTreeMap::default(),
            })),
        }
    }

    pub async fn put_node(&self, node: Node) {
        let mut guard = self.core.lock().await;
        guard.endpoints.insert(node.id, endpoint(&node));
    }

    pub async fn get(&self, node: u64) -> anyhow::Result<Channel> {
        let mut guard = self.core.lock().await;
        if let Some(channel) = guard.channels.get(&node) {
            return Ok(channel.clone());
        }
        if let Some(endpoint) = guard.endpoints.get(&node) {
            let channel = endpoint.connect().await?;
            guard.channels.insert(node, channel.clone());
            return Ok(channel);
        }
        Err(anyhow::anyhow!("endpoint of node {} not found", node))
    }

    pub async fn release(&self, node: u64) -> anyhow::Result<()> {
        let mut guard = self.core.lock().await;
        match guard.channels.remove(&node) {
            Some(_) => Ok(()),
            None => Err(anyhow::anyhow!("channel to node {} not exists", node)),
        }
    }
}
