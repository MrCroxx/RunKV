use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use tracing::trace;

use super::command::Command;

struct ReadyItem {
    index: u64,
    cmds: Vec<Command>,
}

struct ReadOnlyCmdPoolCore {
    /// { id -> [cmd] }
    pending: Mutex<HashMap<u64, Vec<Command>>>,
    ready: Mutex<Vec<ReadyItem>>,
}

#[derive(Clone)]
pub struct ReadOnlyCmdPool {
    core: Arc<ReadOnlyCmdPoolCore>,
}

impl Default for ReadOnlyCmdPool {
    fn default() -> Self {
        Self {
            core: Arc::new(ReadOnlyCmdPoolCore {
                pending: Mutex::new(HashMap::new()),
                ready: Mutex::new(vec![]),
            }),
        }
    }
}

impl ReadOnlyCmdPool {
    pub fn append(&self, id: u64, cmds: Vec<Command>) {
        assert!(self.core.pending.lock().insert(id, cmds).is_none());
    }

    pub fn ready(&self, id: u64, index: u64) {
        let cmds = match self.core.pending.lock().remove(&id) {
            None => {
                trace!("no read-only cmds found at: {}", index);
                return;
            }
            Some(cmds) => cmds,
        };
        let item = ReadyItem { index, cmds };
        let mut ready = self.core.ready.lock();
        if let Some(last) = ready.last() {
            assert!(last.index <= index);
        }
        ready.push(item);
    }

    pub fn split(&self, index: u64) -> Vec<Command> {
        let mut ready = self.core.ready.lock();
        let p = ready.partition_point(|item| item.index <= index);
        let cmds = ready.drain(..p).flat_map(|item| item.cmds).collect_vec();
        cmds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<ReadOnlyCmdPool>();
    }
}
