use std::io::Cursor;
use std::ops::Range;

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
    async fn apply(&self, _index: u64, _request: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn post_apply(&self, range: Range<u64>) -> Result<()> {
        trace!("notify apply to (exclusive): {}", range.end);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::ApplyToExclusive(range.end),
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::ApplyToExclusive(_) => Ok(()),
            _ => unreachable!(),
        }
    }

    async fn build_snapshot(&self, index: u64) -> Result<Cursor<Vec<u8>>> {
        trace!("build snapshot");
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::BuildSnapshot(index),
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::BuildSnapshot(_, snapshot) => Ok(Cursor::new(snapshot)),
            _ => unreachable!(),
        }
    }

    async fn install_snapshot(&self, index: u64, snapshot: &Cursor<Vec<u8>>) -> Result<()> {
        trace!("install snapshot: {:?}", snapshot);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::InstallSnapshot(index, snapshot.to_owned().into_inner()),
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::InstallSnapshot(_) => Ok(()),
            _ => unreachable!(),
        }
    }
}
