use std::collections::btree_map::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use runkv_common::channel_pool::ChannelPool;
use runkv_common::coding::CompressionAlgorithm;
use runkv_common::notify_pool::NotifyPool;
use runkv_common::time::rtimestamp;
use runkv_common::tracing_slog_drain::TracingSlogDrain;
use runkv_common::Worker;
use runkv_proto::kv::TxnResponse;
use runkv_storage::components::SstableStoreRef;
use runkv_storage::manifest::VersionManager;
use runkv_storage::raft_log_store::RaftLogStore;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::error;

use super::fsm::{ObjectLsmTreeFsm, ObjectLsmTreeFsmOptions};
use super::lsm_tree::{ObjectStoreLsmTree, ObjectStoreLsmTreeOptions};
use super::raft_log_store::RaftGroupLogStore;
use super::raft_network::{GrpcRaftNetwork, RaftNetwork};
use crate::error::{Error, RaftManageError, Result};
use crate::worker::raft::{Proposal, RaftStartMode, RaftWorker, RaftWorkerOptions};
use crate::worker::sstable_uploader::{SstableUploader, SstableUploaderOptions};

#[derive(Clone)]
pub struct LsmTreeOptions {
    pub write_buffer_capacity: usize,
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
    pub raft_network: GrpcRaftNetwork,
    pub txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    pub version_manager: VersionManager,
    pub sstable_store: SstableStoreRef,
    pub channel_pool: ChannelPool,
    pub lsm_tree_options: LsmTreeOptions,
}

struct RaftManagerInner {
    proposal_txs: BTreeMap<u64, mpsc::UnboundedSender<Proposal>>,
    raft_worker_handles: BTreeMap<u64, JoinHandle<()>>,
    sstable_uploader_handles: BTreeMap<u64, JoinHandle<()>>,
    sequences: BTreeMap<u64, Arc<AtomicU64>>,
}

#[derive(Clone)]
pub struct RaftManager {
    node: u64,
    rudder_node_id: u64,

    raft_log_store: RaftLogStore,
    raft_network: GrpcRaftNetwork,
    raft_logger_root: slog::Logger,

    txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    version_manager: VersionManager,
    sstable_store: SstableStoreRef,
    channel_pool: ChannelPool,

    lsm_tree_options: LsmTreeOptions,

    inner: Arc<RwLock<RaftManagerInner>>,
}

impl RaftManager {
    pub fn new(options: RaftManagerOptions) -> Self {
        let raft_logger_root =
            slog::Logger::root(TracingSlogDrain, slog::o!("namespace" => "raft"));

        Self {
            node: options.node,
            rudder_node_id: options.rudder_node_id,
            raft_log_store: options.raft_log_store,
            raft_network: options.raft_network,
            raft_logger_root,
            txn_notify_pool: options.txn_notify_pool,
            version_manager: options.version_manager,
            sstable_store: options.sstable_store,
            lsm_tree_options: options.lsm_tree_options,
            channel_pool: options.channel_pool,
            inner: Arc::new(RwLock::new(RaftManagerInner {
                proposal_txs: BTreeMap::default(),
                raft_worker_handles: BTreeMap::default(),
                sstable_uploader_handles: BTreeMap::default(),
                sequences: BTreeMap::default(),
            })),
        }
    }

    pub async fn register(&self, group: u64, raft_nodes: BTreeMap<u64, u64>) -> Result<()> {
        self.raft_network.register(group, raft_nodes).await
    }

    pub async fn create_raft_node(&self, group: u64, raft_node: u64) -> Result<()> {
        // Build Lsm-tree.
        let lsm_tree_options = ObjectStoreLsmTreeOptions {
            sstable_store: self.sstable_store.clone(),
            write_buffer_capacity: self.lsm_tree_options.write_buffer_capacity,
            version_manager: self.version_manager.clone(),
        };
        let lsm_tree = ObjectStoreLsmTree::new(lsm_tree_options);

        // Build raft group log store.
        self.raft_log_store.add_group(raft_node).await?;
        let raft_log_store = RaftGroupLogStore::new(raft_node, self.raft_log_store.clone());

        // Build FSM.
        let fsm_options = ObjectLsmTreeFsmOptions {
            node: self.node,
            group,
            raft_node,
            raft_log_store: raft_log_store.clone(),
            lsm_tree: lsm_tree.clone(),
            txn_notify_pool: self.txn_notify_pool.clone(),
        };
        let fsm = ObjectLsmTreeFsm::new(fsm_options);

        // Build raft logger.
        let raft_logger = self
            .raft_logger_root
            .new(slog::o!("group" => group, "raft node" => raft_node));

        // Build raft worker.
        let peers = self.raft_network.raft_nodes(group).await?;
        let (proposal_tx, proposal_rx) = mpsc::unbounded_channel();
        let raft_worker_options = RaftWorkerOptions {
            group,
            node: self.node,
            raft_node,

            raft_start_mode: RaftStartMode::Initialize { peers },
            raft_log_store,
            raft_logger,
            raft_network: self.raft_network.clone(),

            proposal_rx,

            fsm,
        };
        let mut raft_worker = RaftWorker::build(raft_worker_options).await?;

        // Bootstrap raft worker.
        let raft_worker_handle = tokio::spawn(async move {
            let result = raft_worker.run().await;
            if let Err(e) = result {
                error!(
                    raft_worker = ?raft_worker,
                    "error raised when running raft worker: {}",
                    e
                );
            }
        });

        // Build and bootstrap sstable uploader.
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
        let sstable_uploader_handle = tokio::spawn(async move {
            let result = sstable_uploader.run().await;
            if let Err(e) = result {
                error!(
                    sstable_uploader = ?sstable_uploader,
                    "error raised when running sstable uploader: {}",
                    e
                );
            }
        });

        let mut guard = self.inner.write().await;
        if guard.proposal_txs.insert(raft_node, proposal_tx).is_some()
            || guard
                .raft_worker_handles
                .insert(raft_node, raft_worker_handle)
                .is_some()
            || guard
                .sstable_uploader_handles
                .insert(raft_node, sstable_uploader_handle)
                .is_some()
            // TODO: Use max sequence from WAL.
            || guard
                .sequences
                .insert(raft_node, Arc::new(AtomicU64::new(rtimestamp())))
                .is_some()
        {
            return Err(Error::Other(format!(
                "`proposal tx` or `raft worker handle` or `sstable uploader handle` or `sequence` of {} already exists",
                raft_node
            )));
        }

        Ok(())
    }

    // TODO: REMOVE ME.
    pub async fn get_proposal_channel(
        &self,
        raft_node: u64,
    ) -> Result<mpsc::UnboundedSender<Proposal>> {
        let inner = self.inner.read().await;
        inner.proposal_txs.get(&raft_node).cloned().ok_or_else(|| {
            RaftManageError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            }
            .into()
        })
    }

    // TODO: REMOVE ME.
    pub async fn get_sequence(&self, raft_node: u64) -> Result<Arc<AtomicU64>> {
        let inner = self.inner.read().await;
        inner.sequences.get(&raft_node).cloned().ok_or_else(|| {
            RaftManageError::RaftNodeNotExists {
                raft_node,
                node: self.node,
            }
            .into()
        })
    }
}
