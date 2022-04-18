use async_trait::async_trait;
use runkv_common::Worker;
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::components::command::{AsyncCommand, CommandRequest, CommandResponse};
use crate::components::gear::Gear;
use crate::components::lsm_tree::ObjectStoreLsmTree;
use crate::components::Raft;
use crate::error::Result;

pub struct KvWorkerOptions {
    pub group: u64,
    pub raft_node: u64,
    pub lsm_tree: ObjectStoreLsmTree,
    pub raft: Raft<Gear>,
    pub rx: mpsc::UnboundedReceiver<AsyncCommand>,
}

pub struct KvWorker {
    _group: u64,
    _raft_node: u64,
    _lsm_tree: ObjectStoreLsmTree,
    _raft: Raft<Gear>,
    rx: mpsc::UnboundedReceiver<AsyncCommand>,
}

#[async_trait]
impl Worker for KvWorker {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }
}

impl KvWorker {
    pub fn new(options: KvWorkerOptions) -> Self {
        Self {
            _group: options.group,
            _raft_node: options.raft_node,
            _lsm_tree: options.lsm_tree,
            _raft: options.raft,
            rx: options.rx,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        // TODO: Handle cmd.
        while let Some(cmd) = self.rx.recv().await {
            trace!("receive cmd: {:?}", cmd);
            let rsp = match cmd.request {
                CommandRequest::Data {
                    group,
                    index,
                    data: _,
                } => CommandResponse::Data { group, index },
                CommandRequest::BuildSnapshot { group, index } => CommandResponse::BuildSnapshot {
                    group,
                    index,
                    snapshot: vec![],
                },
                CommandRequest::InstallSnapshot {
                    group,
                    index,
                    snapshot: _,
                } => CommandResponse::InstallSnapshot { group, index },
            };
            cmd.response.send(Ok(rsp)).unwrap();
        }
        Ok(())
    }
}
