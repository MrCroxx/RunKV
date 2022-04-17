use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use parking_lot::RwLock;

use crate::error::Result;

#[async_trait]
pub trait Fsm: Send + Sync + Clone + 'static {
    async fn apply(&self, index: u64, request: &[u8]) -> Result<()>;

    async fn build_snapshot(&self, index: u64) -> Result<Cursor<Vec<u8>>>;

    async fn install_snapshot(&self, index: u64, snapshot: &Cursor<Vec<u8>>) -> Result<()>;

    async fn pre_apply(&self, _range: Range<u64>) -> Result<()> {
        Ok(())
    }

    async fn post_apply(&self, _range: Range<u64>) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MockKvFsm {
    state: Arc<RwLock<(u64, Vec<u8>)>>,
}

impl Default for MockKvFsm {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new((0, Vec::new()))),
        }
    }
}

#[async_trait]
impl Fsm for MockKvFsm {
    async fn apply(&self, index: u64, request: &[u8]) -> Result<()> {
        let mut state = self.state.write();
        *state = (index, request.to_vec());
        Ok(())
    }

    async fn build_snapshot(&self, _index: u64) -> Result<Cursor<Vec<u8>>> {
        let state = self.state.read();
        let mut buf = state.1.to_vec();
        buf.put_u64_le(state.0);
        Ok(Cursor::new(buf))
    }

    async fn install_snapshot(&self, _index: u64, snapshot: &Cursor<Vec<u8>>) -> Result<()> {
        let mut state = self.state.write();
        let mut buf = snapshot.clone().into_inner();
        let index = (&buf[buf.len() - 8..]).get_u64_le();
        buf.truncate(buf.len() - 8);
        *state = (index, buf);
        Ok(())
    }
}
