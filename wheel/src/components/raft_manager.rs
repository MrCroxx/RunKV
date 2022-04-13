use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::Arc;

use runkv_storage::raft_log_store::RaftLogStore;
use tokio::sync::RwLock;

use super::gear::Gear;
use super::network::RaftNetwork;
use super::raft_log_store::RaftGroupLogStore;
use super::Raft;
use crate::error::{RaftError, Result};

pub struct RaftManagerOptions {
    pub raft_log_store: RaftLogStore,
    pub raft_network: RaftNetwork,
    pub gear: Gear,
    pub node: u64,
}

pub struct RaftManager {
    _node: u64,
    raft_log_store: RaftLogStore,
    raft_network: RaftNetwork,
    gear: Gear,
    rafts: Arc<RwLock<BTreeMap<u64, Raft>>>,
}

impl RaftManager {
    pub fn new(options: RaftManagerOptions) -> Self {
        Self {
            _node: options.node,
            raft_log_store: options.raft_log_store,
            raft_network: options.raft_network,
            gear: options.gear,
            rafts: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }

    pub async fn add_raft_group(
        &self,
        group: u64,
        raft_node: u64,
        raft_nodes: BTreeMap<u64, u64>,
    ) -> Result<()> {
        let mut rafts = self.rafts.write().await;
        match rafts.entry(raft_node) {
            Entry::Occupied(_) => Err(RaftError::RaftNodeAlreadyExists {
                group,
                node: raft_node,
            }
            .into()),
            Entry::Vacant(v) => {
                for (raft_node, node) in raft_nodes.iter() {
                    self.raft_network.update_raft_node(*node, *raft_node)?;
                }
                let network = self.raft_network.clone();
                let storage =
                    RaftGroupLogStore::new(group, self.raft_log_store.clone(), self.gear.clone());
                let config = openraft::Config {
                    cluster_name: format!("raft-group-{}", group),
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
                let config = Arc::new(config);
                let raft = Raft::new(raft_node, config, network, storage);
                v.insert(raft);
                Ok(())
            }
        }
    }
}
