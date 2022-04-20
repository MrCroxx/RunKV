use async_trait::async_trait;
use runkv_common::Worker;
use tokio::sync::mpsc;
use tracing::warn;

use crate::components::command::{Apply, Snapshot};
use crate::components::gear::Gear;
use crate::components::lsm_tree::ObjectStoreLsmTree;
use crate::components::raft_log_store::RaftGroupLogStore;
use crate::components::Raft;
use crate::error::{Error, Result};

/// Done index (exclusive).
pub const DONE_INDEX_KEY: &[u8] = b"done_index";
/// Available index (exclusive).
pub const AVAILABLE_INDEX_KEY: &[u8] = b"available_index";

pub struct KvWorkerOptions {
    pub group: u64,
    pub raft_node: u64,
    pub available_index: u64,
    pub done_index: u64,
    pub raft_group_log_store: RaftGroupLogStore<Gear>,
    pub lsm_tree: ObjectStoreLsmTree,
    pub raft: Raft<Gear>,
    pub apply_rx: mpsc::UnboundedReceiver<Apply>,
    pub snapshot_rx: mpsc::UnboundedReceiver<Snapshot>,
}

pub struct KvWorker {
    group: u64,
    raft_node: u64,
    raft_group_log_store: RaftGroupLogStore<Gear>,
    lsm_tree: ObjectStoreLsmTree,
    raft: Raft<Gear>,
    available_index: u64,
    done_index: u64,
    apply_rx: mpsc::UnboundedReceiver<Apply>,
    snapshot_rx: mpsc::UnboundedReceiver<Snapshot>,
}

#[async_trait]
impl Worker for KvWorker {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => return Ok(()),
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }
}

impl KvWorker {
    pub fn new(options: KvWorkerOptions) -> Self {
        Self {
            group: options.group,
            raft_node: options.raft_node,
            raft_group_log_store: options.raft_group_log_store,
            lsm_tree: options.lsm_tree,
            raft: options.raft,
            available_index: options.available_index,
            done_index: options.done_index,
            apply_rx: options.apply_rx,
            snapshot_rx: options.snapshot_rx,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;

                Some(snapshot) = self.snapshot_rx.recv() => {
                    self.handle_snapshot(snapshot).await?;
                }
                Some(apply) = self.apply_rx.recv() => {
                    self.handle_apply(apply).await?;
                }
                else => return Ok(()),
            }
        }
    }

    async fn handle_apply(&mut self, apply: Apply) -> Result<()> {
        debug_assert_eq!(self.group, apply.group);
        assert!(self.available_index <= apply.range.end);
        self.available_index = apply.range.end;
        Ok(())
    }

    async fn handle_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        match snapshot {
            Snapshot::Build {
                group,
                index,
                notifier,
            } => {
                let snapshot = self.handle_build_snapshot(group, index).await?;
                notifier
                    .send(snapshot)
                    .map_err(|_| Error::Other("notify build snapshot error".to_string()))?;
            }
            Snapshot::Install {
                group,
                index,
                snapshot,
                notifier,
            } => {
                self.handle_install_snapshot(group, index, snapshot).await?;
                notifier
                    .send(())
                    .map_err(|_| Error::Other("notify install snapshot error".to_string()))?;
            }
        }
        Ok(())
    }

    async fn handle_build_snapshot(&mut self, _group: u64, _indexx: u64) -> Result<Vec<u8>> {
        // TODO: Impl me.
        Ok(vec![])
    }

    async fn handle_install_snapshot(
        &mut self,
        _group: u64,
        _index: u64,
        _snapshot: Vec<u8>,
    ) -> Result<()> {
        // TODO: Impl me.
        Ok(())
    }

    async fn write_next_apply_index(&self, index: u64) -> Result<()> {
        let buf = bincode::serialize(&index).map_err(Error::serde_err)?;
        self.raft_group_log_store
            .put(DONE_INDEX_KEY.to_vec(), buf)
            .await?;
        Ok(())
    }

    async fn read_next_apply_index(&self) -> Result<Option<u64>> {
        let raw = self
            .raft_group_log_store
            .get(DONE_INDEX_KEY.to_vec())
            .await?;
        let index = match raw {
            Some(raw) => bincode::deserialize(&raw).map_err(Error::serde_err)?,
            None => None,
        };
        Ok(index)
    }
}
