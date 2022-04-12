use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::{RaftRequest, RaftResponse};
use crate::error::Result;

#[async_trait]
pub trait Fsm: Send + Sync + Clone + 'static {
    async fn apply(&self, request: &RaftRequest) -> Result<RaftResponse>;
    async fn build_snapshot(&self) -> Result<Cursor<Vec<u8>>>;
    async fn apply_snapshot(&self, snapshot: &Cursor<Vec<u8>>) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct MockFsm {
    state: Arc<RwLock<Vec<u8>>>,
}

impl Default for MockFsm {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Fsm for MockFsm {
    async fn apply(&self, request: &RaftRequest) -> Result<RaftResponse> {
        let mut state = self.state.write().await;
        *state = bincode::serialize(request).unwrap();
        Ok(RaftResponse {})
    }

    async fn build_snapshot(&self) -> Result<Cursor<Vec<u8>>> {
        let state = self.state.read().await;
        let buf = (*state).clone();
        Ok(Cursor::new(buf))
    }

    async fn apply_snapshot(&self, snapshot: &Cursor<Vec<u8>>) -> Result<()> {
        let mut state = self.state.write().await;
        *state = snapshot.clone().into_inner();
        Ok(())
    }
}
