use std::io::Cursor;
use std::ops::Range;

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Fsm: Send + Sync + Clone + 'static {
    async fn apply(&self, group: u64, index: u64, request: &[u8]) -> Result<()>;

    async fn build_snapshot(&self, group: u64, index: u64) -> Result<Cursor<Vec<u8>>>;

    async fn install_snapshot(
        &self,
        group: u64,
        index: u64,
        snapshot: &Cursor<Vec<u8>>,
    ) -> Result<()>;

    async fn pre_apply(&self, _group: u64, _range: Range<u64>) -> Result<()> {
        Ok(())
    }

    async fn post_apply(&self, _group: u64, _range: Range<u64>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use std::sync::Arc;

    use bytes::{Buf, BufMut};
    use parking_lot::RwLock;

    use super::*;

    #[derive(Clone, Debug)]
    pub struct MockFsm {
        state: Arc<RwLock<(u64, u64, Vec<u8>)>>,
    }

    impl Default for MockFsm {
        fn default() -> Self {
            Self {
                state: Arc::new(RwLock::new((0, 0, Vec::new()))),
            }
        }
    }

    #[async_trait]
    impl Fsm for MockFsm {
        async fn apply(&self, group: u64, index: u64, request: &[u8]) -> Result<()> {
            let mut state = self.state.write();
            *state = (group, index, request.to_vec());
            Ok(())
        }

        async fn build_snapshot(&self, _group: u64, _index: u64) -> Result<Cursor<Vec<u8>>> {
            let state = self.state.read();
            let mut buf = state.2.to_vec();
            buf.put_u64_le(state.0);
            buf.put_u64_le(state.1);
            Ok(Cursor::new(buf))
        }

        async fn install_snapshot(
            &self,
            _group: u64,
            _index: u64,
            snapshot: &Cursor<Vec<u8>>,
        ) -> Result<()> {
            let mut state = self.state.write();
            let mut buf = snapshot.clone().into_inner();
            let mut cursor = buf.len();
            cursor -= 8;
            let index = (&buf[cursor..cursor + 8]).get_u64_le();
            cursor -= 8;
            let group = (&buf[cursor..cursor + 8]).get_u64_le();
            buf.truncate(cursor);
            *state = (group, index, buf);
            Ok(())
        }
    }
}
