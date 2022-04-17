use std::collections::btree_map::BTreeMap;
use std::sync::Arc;

use runkv_storage::raft_log_store::RaftLogStore;
use tokio::sync::RwLock;

use super::gear::Gear;
use super::network::RaftNetwork;
use super::raft_log_store::RaftGroupLogStore;
use super::Raft;
use crate::error::{RaftError, Result};

const RAFT_GROUP_NAME_PREFIX: &str = "raft-group-";

pub struct RaftManagerOptions {
    pub raft_log_store: RaftLogStore,
    pub raft_network: RaftNetwork,
    pub gear: Gear,
    pub node: u64,
}

#[derive(Default)]
struct RaftManagerInner {
    /// `{ raft node id -> Raft }`.
    rafts: BTreeMap<u64, Raft>,
}

#[derive(Clone)]
pub struct RaftManager {
    node: u64,
    raft_log_store: RaftLogStore,
    raft_network: RaftNetwork,
    gear: Gear,
    inner: Arc<RwLock<RaftManagerInner>>,
}

impl RaftManager {
    pub fn new(options: RaftManagerOptions) -> Self {
        Self {
            node: options.node,
            raft_log_store: options.raft_log_store,
            raft_network: options.raft_network,
            gear: options.gear,
            inner: Arc::new(RwLock::new(RaftManagerInner::default())),
        }
    }

    // TODO: Refactor me.
    pub async fn update_routers(&self, routers: BTreeMap<u64, u64>) -> Result<()> {
        for (raft_node, node) in routers.into_iter() {
            self.raft_network.update_raft_node(node, raft_node)?;
        }
        Ok(())
    }

    pub async fn add_raft_node(&self, group: u64, raft_node: u64) -> Result<()> {
        let mut inner = self.inner.write().await;

        if inner.rafts.get(&raft_node).is_some() {
            return Err(RaftError::RaftNodeAlreadyExists {
                group,
                raft_node,
                node: self.node,
            }
            .into());
        }

        let network = self.raft_network.clone();
        self.raft_log_store.add_group(raft_node).await?;
        let storage = RaftGroupLogStore::new(group, self.raft_log_store.clone(), self.gear.clone());
        let config = openraft::Config {
            cluster_name: raft_group_name(group),
            // election_timeout_min: todo!(),
            // election_timeout_max: todo!(),
            // heartbeat_interval: todo!(),
            // install_snapshot_timeout: todo!(),
            // max_payload_entries: todo!(),
            // replication_lag_threshold: todo!(),
            // snapshot_policy: todo!(),
            // snapshot_max_chunk_size: todo!(),
            // max_applied_log_to_keep: todo!(),
            ..Default::default()
        };
        let config = config.validate().map_err(RaftError::err)?;
        let config = Arc::new(config);

        let raft = Raft::new(raft_node, config, network, storage);
        inner.rafts.insert(raft_node, raft);

        Ok(())
    }

    pub async fn get_raft_node(&self, raft_node: u64) -> Result<Raft> {
        let inner = self.inner.read().await;
        inner.rafts.get(&raft_node).cloned().ok_or_else(|| {
            RaftError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            }
            .into()
        })
    }
}

fn raft_group_name(group: u64) -> String {
    format!("{}{}", RAFT_GROUP_NAME_PREFIX, group)
}

#[allow(dead_code)]
fn raft_group_id(s: &str) -> u64 {
    s[RAFT_GROUP_NAME_PREFIX.len()..].parse().unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::SocketAddr;

    use runkv_common::channel_pool::ChannelPool;
    use runkv_common::config::Node;
    use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
    use test_log::test;
    use tokio::sync::mpsc;
    use tracing::trace;

    use super::*;
    use crate::components::command::{AsyncCommand, CommandRequest, CommandResponse};
    use crate::service::tests::MockRaftService;

    #[test(tokio::test)]
    async fn test_raft() {
        let tempdir = tempfile::tempdir().unwrap();
        let addr_str = "127.0.0.1:12399".to_string();
        let (manager, mut rx) =
            build_manager_for_test(tempdir.path().to_str().unwrap(), addr_str.parse().unwrap())
                .await;
        let _rx_handle = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                trace!("receive cmd: {:?}", cmd);
                let rsp = match cmd.request {
                    CommandRequest::ApplyToExclusive(index) => {
                        CommandResponse::ApplyToExclusive(index)
                    }
                    CommandRequest::BuildSnapshot(index) => {
                        CommandResponse::BuildSnapshot(index, vec![])
                    }
                    CommandRequest::InstallSnapshot(index, _) => {
                        CommandResponse::InstallSnapshot(index)
                    }
                };
                cmd.response.send(Ok(rsp)).unwrap();
            }
        });
        let manager_clone = manager.clone();
        let _raft_service_handle = tokio::spawn(MockRaftService::bootstrap(
            manager_clone,
            addr_str.parse().unwrap(),
        ));

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        manager
            .update_routers(BTreeMap::from_iter([(1, 1), (2, 1), (3, 1)].into_iter()))
            .await
            .unwrap();
        manager.add_raft_node(1, 1).await.unwrap();
        manager.add_raft_node(1, 2).await.unwrap();
        manager.add_raft_node(1, 3).await.unwrap();

        let leader = manager.get_raft_node(1).await.unwrap();
        let follower1 = manager.get_raft_node(2).await.unwrap();
        let follower2 = manager.get_raft_node(3).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        leader
            .initialize(BTreeSet::from_iter([1, 2, 3].into_iter()))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        assert!(leader.is_leader().await.is_ok());
        assert!(follower1.is_leader().await.is_err());
        assert!(follower2.is_leader().await.is_err());
    }

    async fn build_manager_for_test(
        path: &str,
        addr: SocketAddr,
    ) -> (RaftManager, mpsc::UnboundedReceiver<AsyncCommand>) {
        let channel_pool = ChannelPool::default();
        let node = Node {
            id: 1,
            host: addr.ip().to_string(),
            port: addr.port(),
        };
        channel_pool.put_node(node).await;
        let (tx, rx) = mpsc::unbounded_channel();
        let raft_log_store_options = RaftLogStoreOptions {
            log_dir_path: path.to_string(),
            log_file_capacity: 128,
            block_cache_capacity: 1024,
        };
        let raft_log_store = RaftLogStore::open(raft_log_store_options).await.unwrap();
        let raft_network = RaftNetwork::new(channel_pool);
        let gear = Gear::new(tx);
        let options = RaftManagerOptions {
            raft_log_store,
            raft_network,
            gear,
            node: 1,
        };
        let manager = RaftManager::new(options);
        (manager, rx)
    }
}
