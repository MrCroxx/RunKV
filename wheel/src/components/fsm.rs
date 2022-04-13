use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use runkv_proto::wheel::{KvRequest, KvResponse};

use crate::error::Result;

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
        let mut state = self.state.write();
        *state = bincode::serialize(request).unwrap();
        Ok(KvResponse { ops: vec![] })
    }

    async fn build_snapshot(&self) -> Result<Cursor<Vec<u8>>> {
        let state = self.state.read();
        let buf = (*state).clone();
        Ok(Cursor::new(buf))
    }

    async fn install_snapshot(&self, snapshot: &Cursor<Vec<u8>>) -> Result<()> {
        let mut state = self.state.write();
        *state = snapshot.clone().into_inner();
        Ok(())
    }
}
