use std::io::Cursor;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

use super::command::{AsyncCommand, CommandRequest, CommandResponse};
use super::fsm::Fsm;
use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Gear {
    sender: mpsc::UnboundedSender<AsyncCommand>,
}

impl Gear {
    pub fn new(sender: mpsc::UnboundedSender<AsyncCommand>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl Fsm for Gear {
    async fn apply(&self, group: u64, index: u64, request: &[u8]) -> Result<()> {
        trace!(group = group, index = index, "notify apply:");
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::Data {
                    group,
                    index,
                    data: request.to_vec(),
                },
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::Data { .. } => Ok(()),
            _ => unreachable!(),
        }
    }

    async fn build_snapshot(&self, group: u64, index: u64) -> Result<Cursor<Vec<u8>>> {
        trace!("build snapshot");
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::BuildSnapshot { group, index },
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::BuildSnapshot { snapshot, .. } => Ok(Cursor::new(snapshot)),
            _ => unreachable!(),
        }
    }

    async fn install_snapshot(
        &self,
        group: u64,
        index: u64,
        snapshot: &Cursor<Vec<u8>>,
    ) -> Result<()> {
        trace!("install snapshot: {:?}", snapshot);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::InstallSnapshot {
                    group,
                    index,
                    snapshot: snapshot.to_owned().into_inner(),
                },
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::InstallSnapshot { .. } => Ok(()),
            _ => unreachable!(),
        }
    }
}
