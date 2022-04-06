use std::sync::Arc;

use crate::log::PipeLog;
use crate::mem::MemStates;

struct RaftLogStoreCore {
    pipe_log: PipeLog,
    states: MemStates,
}

#[derive(Clone)]
pub struct RaftLogStore {
    core: Arc<RaftLogStoreCore>,
}

impl RaftLogStore {
    pub fn new(pipe_log: PipeLog, states: MemStates) -> Self {
        Self {
            core: Arc::new(RaftLogStoreCore { pipe_log, states }),
        }
    }
}
