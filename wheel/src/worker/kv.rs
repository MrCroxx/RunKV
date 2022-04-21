use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runkv_common::Worker;
use runkv_proto::kv::{BytesSerde, TxnRequest};
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::components::command::Command;
use crate::components::gear::Gear;
use crate::components::lsm_tree::ObjectStoreLsmTree;
use crate::components::raft_log_store::RaftGroupLogStore;
use crate::components::{Raft, RaftTypeConfig};
use crate::error::{Error, Result};

/// Done index (inclusive).
pub const DONE_INDEX_KEY: &[u8] = b"done_index";
/// Available index (inclusive).
pub const AVAILABLE_INDEX_KEY: &[u8] = b"available_index";

const APPLIER_SUSPEND_INTERVAL: Duration = Duration::from_millis(100);
const APPLIER_MAX_BATCH_SIZE: usize = 100;

pub struct KvWorkerOptions {
    pub group: u64,
    pub raft_node: u64,
    pub available_index: u64,
    pub done_index: u64,
    pub raft_group_log_store: RaftGroupLogStore<Gear>,
    pub lsm_tree: ObjectStoreLsmTree,
    pub raft: Raft<Gear>,
    pub rx: mpsc::UnboundedReceiver<Command>,
}

pub struct KvWorker {
    group: u64,
    raft_node: u64,
    raft_group_log_store: RaftGroupLogStore<Gear>,
    lsm_tree: ObjectStoreLsmTree,
    raft: Raft<Gear>,

    available_index: Arc<AtomicU64>,
    done_index: Arc<AtomicU64>,
    snapshotting: Arc<AtomicU64>,

    rx: mpsc::UnboundedReceiver<Command>,
}

#[async_trait]
impl Worker for KvWorker {
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut applier = Applier {
            group: self.group,
            raft_node: self.raft_node,
            raft_group_log_store: self.raft_group_log_store.clone(),
            lsm_tree: self.lsm_tree.clone(),
            raft: self.raft.clone(),
            available_index: self.available_index.clone(),
            done_index: self.done_index.clone(),
            snapshotting: self.snapshotting.clone(),
        };
        // TODO: Gracefully kill.
        let _handle = tokio::spawn(async move { applier.run().await });

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
            available_index: Arc::new(AtomicU64::new(options.available_index)),
            done_index: Arc::new(AtomicU64::new(options.done_index)),
            snapshotting: Arc::new(AtomicU64::new(0)),
            rx: options.rx,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        while let Some(cmd) = self.rx.recv().await {
            trace!(
                group = self.group,
                raft_node = self.raft_node,
                "receive cmd: {:?}",
                cmd
            );
            match cmd {
                Command::Apply { group, range } => {
                    self.handle_apply(group, range).await?;
                }
                Command::BuildSnapshot {
                    group,
                    index,
                    notifier,
                } => {
                    let snapshot = self.handle_build_snapshot(group, index).await?;
                    notifier.send(snapshot).map_err(|_| {
                        Error::Other("error raised to notify build snapshot".to_string())
                    })?;
                }
                Command::InstallSnapshot {
                    group,
                    index,
                    snapshot,
                    notifier,
                } => {
                    self.handle_install_snapshot(group, index, snapshot).await?;
                    notifier.send(()).map_err(|_| {
                        Error::Other("error raised to notify build snapshot".to_string())
                    })?;
                }
            }
        }
        Ok(())
    }

    async fn handle_apply(&mut self, group: u64, range: Range<u64>) -> Result<()> {
        debug_assert_eq!(self.group, group);
        let old = self.available_index.swap(range.end - 1, Ordering::Release);
        assert!(old <= range.end);
        Ok(())
    }

    async fn handle_build_snapshot(&mut self, group: u64, index: u64) -> Result<Vec<u8>> {
        debug_assert_eq!(self.group, group);
        self.snapshotting.store(index, Ordering::Release);
        // TODO: Impl me.
        // TODO: Impl me.
        // TODO: Impl me.
        self.snapshotting.store(0, Ordering::Release);
        Ok(vec![])
    }

    async fn handle_install_snapshot(
        &mut self,
        group: u64,
        index: u64,
        _snapshot: Vec<u8>,
    ) -> Result<()> {
        debug_assert_eq!(self.group, group);
        self.snapshotting.store(index, Ordering::Release);
        // TODO: Impl me.
        // TODO: Impl me.
        // TODO: Impl me.
        self.snapshotting.store(0, Ordering::Release);
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

struct Applier {
    group: u64,
    raft_node: u64,
    raft_group_log_store: RaftGroupLogStore<Gear>,
    lsm_tree: ObjectStoreLsmTree,
    raft: Raft<Gear>,

    available_index: Arc<AtomicU64>,
    done_index: Arc<AtomicU64>,
    snapshotting: Arc<AtomicU64>,
}

impl Applier {
    async fn run(&mut self) -> Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => return Ok(()),
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        loop {
            // Suspend for a while if:
            //   1. [`Applier`] is not serving raft group leader.
            //   2. [`KvWorker`] is building/installing snapshot.
            if self.raft.is_leader().await.is_err()
                || self.snapshotting.load(Ordering::Acquire) != 0
            {
                tokio::time::sleep(APPLIER_SUSPEND_INTERVAL).await;
                continue;
            }

            let done = self.done_index.load(Ordering::Acquire);
            let available = self.available_index.load(Ordering::Acquire);

            // Yield back to tokio runtime if there is not log to apply.
            if done >= available {
                tokio::task::yield_now().await;
                continue;
            }

            let entries = self
                .raft_group_log_store
                .entries(done + 1, APPLIER_MAX_BATCH_SIZE)
                .await?;
            let done = done + entries.len() as u64;

            let mut txns = Vec::with_capacity(entries.len());
            for entry in entries {
                let data =
                    match bincode::deserialize::<openraft::EntryPayload<RaftTypeConfig>>(&entry)
                        .map_err(Error::serde_err)?
                    {
                        openraft::EntryPayload::Normal(data) => data,
                        _ => continue,
                    };
                let txn = TxnRequest::from_slice(&data).map_err(Error::serde_err)?;
                txns.push(txn);
            }

            // TODO: Handle txns.

            self.done_index.store(done, Ordering::Release);
        }
    }
}
