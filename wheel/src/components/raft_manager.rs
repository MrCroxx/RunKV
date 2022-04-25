use std::collections::btree_map::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use runkv_common::channel_pool::ChannelPool;
use runkv_common::coding::CompressionAlgorithm;
use runkv_common::notify_pool::NotifyPool;
use runkv_common::time::rtimestamp;
use runkv_common::Worker;
use runkv_proto::kv::TxnResponse;
use runkv_storage::components::SstableStoreRef;
use runkv_storage::manifest::VersionManager;
use runkv_storage::raft_log_store::RaftLogStore;
use tokio::sync::{mpsc, RwLock};

use super::gear::Gear;
use super::lsm_tree::{ObjectStoreLsmTree, ObjectStoreLsmTreeOptions};
use super::network::RaftNetwork;
use super::raft_log_store::RaftGroupLogStore;
use super::Raft;
use crate::error::{Error, RaftError, Result};
use crate::worker::kv::{KvWorker, KvWorkerOptions, DONE_INDEX_KEY};
use crate::worker::sstable_uploader::{SstableUploader, SstableUploaderOptions};

const RAFT_GROUP_NAME_PREFIX: &str = "raft-group-";

#[derive(Clone)]
pub struct LsmTreeOptions {
    pub shard_write_buffer_capacity: usize,
    pub sstable_capacity: usize,
    pub block_capacity: usize,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub compression_algorithm: CompressionAlgorithm,
    pub poll_interval: Duration,
}

pub struct RaftManagerOptions {
    pub node: u64,
    pub rudder_node_id: u64,
    pub raft_log_store: RaftLogStore,
    pub raft_network: RaftNetwork,
    pub txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    pub version_manager: VersionManager,
    pub sstable_store: SstableStoreRef,
    pub channel_pool: ChannelPool,
    pub lsm_tree_options: LsmTreeOptions,
}

struct RaftManagerInner {
    /// `{ raft node id -> Raft }`.
    rafts: BTreeMap<u64, Raft<Gear>>,
    /// `{raft node id -> ObjectStoreLsmTree }`
    lsm_trees: BTreeMap<u64, ObjectStoreLsmTree>,
    /// `{raft node id -> sequence }`
    sequences: BTreeMap<u64, Arc<AtomicU64>>,
}

#[derive(Clone)]
pub struct RaftManager {
    node: u64,
    rudder_node_id: u64,

    raft_log_store: RaftLogStore,
    raft_network: RaftNetwork,
    txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    version_manager: VersionManager,
    sstable_store: SstableStoreRef,
    channel_pool: ChannelPool,

    lsm_tree_options: LsmTreeOptions,

    inner: Arc<RwLock<RaftManagerInner>>,
}

impl RaftManager {
    pub fn new(options: RaftManagerOptions) -> Self {
        Self {
            node: options.node,
            rudder_node_id: options.rudder_node_id,
            raft_log_store: options.raft_log_store,
            raft_network: options.raft_network,
            txn_notify_pool: options.txn_notify_pool,
            version_manager: options.version_manager,
            sstable_store: options.sstable_store,
            lsm_tree_options: options.lsm_tree_options,
            channel_pool: options.channel_pool,
            inner: Arc::new(RwLock::new(RaftManagerInner {
                rafts: BTreeMap::default(),
                lsm_trees: BTreeMap::default(),
                sequences: BTreeMap::default(),
            })),
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

        // Initialize sequence.
        // TODO: Use max sequence from WAL.
        inner
            .sequences
            .insert(raft_node, Arc::new(AtomicU64::new(rtimestamp())));

        // Build LSM-tree.
        let lsm_tree_options = ObjectStoreLsmTreeOptions {
            sstable_store: self.sstable_store.clone(),
            write_buffer_capacity: self.lsm_tree_options.shard_write_buffer_capacity,
            version_manager: self.version_manager.clone(),
        };
        let lsm_tree = ObjectStoreLsmTree::new(lsm_tree_options);
        inner.lsm_trees.insert(raft_node, lsm_tree.clone());

        // Build and run sstable uploader.
        let sstable_uploader_options = SstableUploaderOptions {
            raft_node,
            rudder_node_id: self.rudder_node_id,
            lsm_tree: lsm_tree.clone(),
            sstable_store: self.sstable_store.clone(),
            version_manager: self.version_manager.clone(),
            sstable_capacity: self.lsm_tree_options.sstable_capacity,
            block_capacity: self.lsm_tree_options.block_capacity,
            restart_interval: self.lsm_tree_options.restart_interval,
            bloom_false_positive: self.lsm_tree_options.bloom_false_positive,
            compression_algorithm: self.lsm_tree_options.compression_algorithm,
            poll_interval: self.lsm_tree_options.poll_interval,
            channel_pool: self.channel_pool.clone(),
        };
        let mut sstable_uploader = SstableUploader::new(sstable_uploader_options);
        // TODO: Hold the handle for gracefully shutdown.
        let _handle = tokio::spawn(async move { sstable_uploader.run().await });

        // Build raft node.
        let network = self.raft_network.clone();
        self.raft_log_store.add_group(raft_node).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        let gear = Gear::new(tx);
        let raft_group_log_store = RaftGroupLogStore::new(group, self.raft_log_store.clone(), gear);
        let config = openraft::Config {
            cluster_name: raft_group_name(group),
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            heartbeat_interval: 100,
            install_snapshot_timeout: 2000,
            max_payload_entries: 1000,
            replication_lag_threshold: 10000,
            // snapshot_policy: todo!(),
            // snapshot_max_chunk_size: todo!(),
            max_applied_log_to_keep: 10000,
            ..Default::default()
        };

        let config = config.validate().map_err(RaftError::err)?;
        let config = Arc::new(config);

        let raft = Raft::new(raft_node, config, network, raft_group_log_store.clone());
        let done_index = match raft_group_log_store.get(DONE_INDEX_KEY.to_vec()).await? {
            Some(raw) => bincode::deserialize(&raw).map_err(Error::serde_err)?,
            None => 0,
        };
        let available_index = raft_group_log_store.applied_index().await?.unwrap_or(0);

        let mut kv_worker = KvWorker::new(KvWorkerOptions {
            group,
            raft_node,
            available_index,
            done_index,
            raft_group_log_store,
            lsm_tree,
            raft: raft.clone(),
            rx,
            txn_notify_pool: self.txn_notify_pool.clone(),
        });

        // TODO: Hold the handle for gracefully shutdown.
        let _handle = tokio::spawn(async move { kv_worker.run().await });

        inner.rafts.insert(raft_node, raft);

        Ok(())
    }

    pub async fn get_raft_node(&self, raft_node: u64) -> Result<Raft<Gear>> {
        let inner = self.inner.read().await;
        inner.rafts.get(&raft_node).cloned().ok_or_else(|| {
            RaftError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            }
            .into()
        })
    }

    pub async fn get_sequence(&self, raft_node: u64) -> Result<Arc<AtomicU64>> {
        let inner = self.inner.read().await;
        inner.sequences.get(&raft_node).cloned().ok_or_else(|| {
            RaftError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            }
            .into()
        })
    }

    pub async fn initialize_raft_group(&self, leader: u64, raft_nodes: &[u64]) -> Result<()> {
        let inner = self.inner.read().await;
        let raft = match inner.rafts.get(&leader) {
            None => {
                return Err(RaftError::RaftNodeNotExists {
                    raft_node: leader,
                    node: self.node,
                }
                .into())
            }
            Some(raft) => raft,
        };
        raft.initialize(BTreeSet::from_iter(raft_nodes.iter().copied()))
            .await
            .map_err(Error::raft_err)?;
        Ok(())
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
    use runkv_common::coding::CompressionAlgorithm;
    use runkv_common::config::{LevelCompactionStrategy, LevelOptions, Node};
    use runkv_proto::kv::{BytesSerde, TxnRequest};
    use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions};
    use runkv_storage::manifest::VersionManagerOptions;
    use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
    use runkv_storage::MemObjectStore;
    use test_log::test;

    use super::*;
    use crate::components::command::Command;
    use crate::components::RaftTypeConfig;
    use crate::service::tests::MockRaftService;

    #[test(tokio::test)]
    async fn test_raft_basic() {
        let tempdir = tempfile::tempdir().unwrap();
        let addr_str = "127.0.0.1:12399".to_string();
        let (manager, raft_log_store, txn_notify_pool) =
            build_manager_for_test(tempdir.path().to_str().unwrap(), addr_str.parse().unwrap())
                .await;
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

        let rx = txn_notify_pool.register(1).unwrap();
        let cmd = Command::TxnRequest {
            request_id: 1,
            sequence: 1,
            request: TxnRequest { ops: vec![] },
        };

        let index = leader
            .client_write(openraft::raft::ClientWriteRequest::new(
                openraft::EntryPayload::Normal(cmd.encode_to_vec().unwrap()),
            ))
            .await
            .unwrap()
            .log_id
            .index;

        rx.await.unwrap().unwrap();

        let data = match bincode::deserialize::<openraft::EntryPayload<RaftTypeConfig>>(
            &raft_log_store.entries(1, index, 1).await.unwrap()[0].data[..],
        )
        .unwrap()
        {
            openraft::EntryPayload::Normal(data) => data,
            _ => unreachable!(),
        };
        assert_eq!(cmd.encode_to_vec().unwrap(), data);
    }

    async fn build_manager_for_test(
        path: &str,
        addr: SocketAddr,
    ) -> (
        RaftManager,
        RaftLogStore,
        NotifyPool<u64, Result<TxnResponse>>,
    ) {
        let channel_pool = ChannelPool::default();
        let txn_notify_pool = NotifyPool::default();
        let node = Node {
            id: 1,
            host: addr.ip().to_string(),
            port: addr.port(),
        };
        channel_pool.put_node(node).await;
        let raft_log_store_options = RaftLogStoreOptions {
            log_dir_path: path.to_string(),
            log_file_capacity: 128,
            block_cache_capacity: 1024,
        };
        let raft_log_store = RaftLogStore::open(raft_log_store_options).await.unwrap();
        let raft_network = RaftNetwork::new(channel_pool.clone());
        let object_store = Arc::new(MemObjectStore::default());
        let sstable_store = Arc::new(SstableStore::new(SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache: BlockCache::new(64 << 10),
            meta_cache_capacity: 64 << 10,
        }));
        let version_manager = VersionManager::new(VersionManagerOptions {
            levels_options: vec![
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::Overlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
                LevelOptions {
                    compaction_strategy: LevelCompactionStrategy::NonOverlap,
                    compression_algorithm: CompressionAlgorithm::None,
                },
            ],
            levels: vec![vec![]; 2],
            sstable_store: sstable_store.clone(),
        });

        let options = RaftManagerOptions {
            node: 1,
            rudder_node_id: 0, // disable rudder
            raft_log_store: raft_log_store.clone(),
            raft_network,
            txn_notify_pool: txn_notify_pool.clone(),
            version_manager,
            sstable_store,
            channel_pool,
            lsm_tree_options: LsmTreeOptions {
                shard_write_buffer_capacity: 4 << 10,
                sstable_capacity: 16 << 10,
                block_capacity: 1 << 10,
                restart_interval: 2,
                bloom_false_positive: 0.1,
                compression_algorithm: CompressionAlgorithm::None,
                poll_interval: Duration::from_secs(10),
            },
        };
        let manager = RaftManager::new(options);
        (manager, raft_log_store, txn_notify_pool)
    }
}
