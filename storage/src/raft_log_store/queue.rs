use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;

use super::file_v2::{ActiveFile, FrozenFile};
use crate::error::Result;
use crate::raft_log_store::error::RaftLogStoreError;

#[derive(Debug)]
pub enum LogFile {
    Active(ActiveFile),
    Frozen(FrozenFile),
}

struct LogQueueCore {
    active: ActiveFile,
    frozens: VecDeque<FrozenFile>,
}

#[derive(Clone)]
pub struct LogQueue {
    node: u64,
    core: Arc<RwLock<LogQueueCore>>,
}

impl std::fmt::Debug for LogQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogQueue")
            .field("node", &self.node)
            .finish()
    }
}

impl LogQueue {
    #[tracing::instrument(level = "trace", err)]
    pub fn init(node: u64, active: ActiveFile, mut frozens: Vec<FrozenFile>) -> Result<Self> {
        frozens.sort_by_key(|frozen| frozen.id());
        if !frozens.is_empty() {
            let mut id = frozens.first().unwrap().id();
            for frozen in frozens.iter() {
                if frozen.id() != id {
                    return Err(RaftLogStoreError::RaftLogFileGap {
                        start: id,
                        end: frozen.id(),
                    }
                    .into());
                }
                id += 1;
            }
            if active.id() != id {
                return Err(RaftLogStoreError::RaftLogFileGap {
                    start: id,
                    end: active.id(),
                }
                .into());
            }
        }
        Ok(Self {
            node,
            core: Arc::new(RwLock::new(LogQueueCore {
                active,
                frozens: VecDeque::from_iter(frozens),
            })),
        })
    }

    #[tracing::instrument(level = "trace")]
    pub async fn rotate(&self, active: ActiveFile, frozen: FrozenFile) {
        let mut core = self.core.write();

        core.active = active;
        core.frozens.push_back(frozen);
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn active(&self) -> ActiveFile {
        self.core.read().active.clone()
    }

    #[tracing::instrument(level = "trace", ret, err)]
    pub fn file(&self, id: u64) -> Result<LogFile> {
        let core = self.core.read();
        if id == core.active.id() {
            return Ok(LogFile::Active(core.active.clone()));
        }
        if core.frozens.is_empty() {
            return Err(RaftLogStoreError::RaftLogFileNotFound(id).into());
        }
        let first = core.frozens[0].id();
        if id < first || (id - first) as usize >= core.frozens.len() {
            return Err(RaftLogStoreError::RaftLogFileNotFound(id).into());
        }
        Ok(LogFile::Frozen(core.frozens[(id - first) as usize].clone()))
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn frozen_file_count(&self) -> usize {
        self.core.read().frozens.len()
    }

    #[tracing::instrument(level = "trace", ret)]
    pub fn frozens(&self) -> Vec<FrozenFile> {
        let frozens = self.core.read().frozens.clone();
        Vec::from_iter(frozens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<LogQueue>();
    }
}
