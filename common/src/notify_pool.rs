use std::collections::hash_map::{Entry, HashMap};
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

struct NotifyPoolCore<I: Eq + Hash + Copy + Clone + Display, R> {
    notifiers: HashMap<I, oneshot::Sender<R>>,
}

pub struct NotifyPool<I: Eq + Hash + Copy + Clone + Display, R> {
    core: Arc<Mutex<NotifyPoolCore<I, R>>>,
}

impl<I: Eq + Hash + Copy + Clone + Display, R> Clone for NotifyPool<I, R> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

impl<I: Eq + Hash + Copy + Clone + Display, R> Default for NotifyPool<I, R> {
    fn default() -> Self {
        Self {
            core: Arc::new(Mutex::new(NotifyPoolCore {
                notifiers: HashMap::default(),
            })),
        }
    }
}

impl<I: Eq + Hash + Copy + Clone + Display, R> NotifyPool<I, R> {
    pub fn register(&self, id: I) -> anyhow::Result<oneshot::Receiver<R>> {
        let (tx, rx) = oneshot::channel();
        match self.core.lock().notifiers.entry(id) {
            Entry::Occupied(_) => return Err(anyhow::anyhow!("id {} already exists", id)),
            Entry::Vacant(v) => {
                v.insert(tx);
            }
        }
        Ok(rx)
    }

    pub fn notify(&self, id: I, result: R) -> anyhow::Result<()> {
        let tx = self
            .core
            .lock()
            .notifiers
            .remove(&id)
            .ok_or_else(|| anyhow::anyhow!("id {} does not exists", id))?;
        tx.send(result)
            .map_err(|_| anyhow::anyhow!("error raised to send result to notifier {}", id))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::*;
    fn is_send_sync<T: Send + Sync + 'static>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<NotifyPool<u64, ()>>();
    }
}
