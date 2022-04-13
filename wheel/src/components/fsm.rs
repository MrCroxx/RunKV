use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use runkv_proto::wheel::{KvRequest, KvResponse};
use tokio::sync::{mpsc, oneshot, RwLock};

use super::command::{AsyncCommand, CommandRequest, CommandResponse};
use crate::error::{Error, Result};

#[async_trait]
pub trait KvFsm: Send + Sync + Clone + 'static {
    async fn apply(&self, request: &KvRequest) -> Result<KvResponse>;
    async fn build_snapshot(&self) -> Result<Cursor<Vec<u8>>>;
    async fn install_snapshot(&self, snapshot: &Cursor<Vec<u8>>) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct MockKvFsm {
    state: Arc<RwLock<Vec<u8>>>,
}

impl Default for MockKvFsm {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl KvFsm for MockKvFsm {
    async fn apply(&self, request: &KvRequest) -> Result<KvResponse> {
        let mut state = self.state.write().await;
        *state = bincode::serialize(request).unwrap();
        Ok(KvResponse { ops: vec![] })
    }

    async fn build_snapshot(&self) -> Result<Cursor<Vec<u8>>> {
        let state = self.state.read().await;
        let buf = (*state).clone();
        Ok(Cursor::new(buf))
    }

    async fn install_snapshot(&self, snapshot: &Cursor<Vec<u8>>) -> Result<()> {
        let mut state = self.state.write().await;
        *state = snapshot.clone().into_inner();
        Ok(())
    }
}

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
impl KvFsm for Gear {
    async fn apply(&self, request: &KvRequest) -> Result<KvResponse> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::Kv(request.to_owned()),
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::Kv(kv) => Ok(kv),
            _ => unreachable!(),
        }
    }

    async fn build_snapshot(&self) -> Result<Cursor<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::BuildSnapshot,
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::BuildSnapshot(snapshot) => Ok(Cursor::new(snapshot)),
            _ => unreachable!(),
        }
    }

    async fn install_snapshot(&self, snapshot: &Cursor<Vec<u8>>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(AsyncCommand {
                request: CommandRequest::InstallSnapshot(snapshot.to_owned().into_inner()),
                response: tx,
            })
            .map_err(Error::err)?;
        let res = rx.await.map_err(Error::err)??;
        match res {
            CommandResponse::InstallSnapshot => Ok(()),
            _ => unreachable!(),
        }
    }
}
