use std::sync::Arc;

use itertools::Itertools;

use crate::entry::{Entry, RaftLogBatch};
use crate::error::Result;
use crate::log::Log;
use crate::mem::{EntryIndex, MemStates};

struct RaftLogStoreCore {
    log: Log,
    states: MemStates,
}

#[derive(Clone)]
pub struct RaftLogStore {
    core: Arc<RaftLogStoreCore>,
}

impl RaftLogStore {
    pub fn new(pipe_log: Log, states: MemStates) -> Self {
        Self {
            core: Arc::new(RaftLogStoreCore {
                log: pipe_log,
                states,
            }),
        }
    }

    pub async fn append_log(&self, batch: RaftLogBatch) -> Result<()> {
        let (data_segment_offset, data_segment_len) = batch.data_segment_location();
        let _group = batch.group();
        let term = batch.term();
        let _first_index = batch.first_index();
        let locations = (0..batch.len())
            .into_iter()
            .map(|i| batch.location(i))
            .collect_vec();

        let entry = Entry::RaftLogBatch(batch);
        let (write_offset, _write_len) = self.core.log.push(entry).await?;

        let _indices = locations
            .into_iter()
            .map(|(offset, len)| EntryIndex {
                term,
                block_offset: write_offset + data_segment_offset,
                block_len: data_segment_len,
                offset,
                len,
            })
            .collect_vec();

        // TODO: Append MemState.

        Ok(())
    }

    pub fn get_log(&self) {
        todo!()
    }

    pub fn put(&self) {
        todo!()
    }

    pub fn delete(&self) {
        todo!()
    }

    pub fn get(&self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<RaftLogStore>()
    }
}
