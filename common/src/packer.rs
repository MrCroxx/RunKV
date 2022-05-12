use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

const DEFAULT_QUEUE_CAPACITY: usize = 64;

pub struct Item<T, R>
where
    T: 'static,
    R: 'static,
{
    pub data: T,
    pub notifier: Option<oneshot::Sender<R>>,
}

struct PackerCore<T, R>
where
    T: 'static,
    R: 'static,
{
    queue: Mutex<Vec<Item<T, R>>>,
}

#[derive(Clone)]
pub struct Packer<T, R>
where
    T: 'static,
    R: 'static,
{
    default_queue_capacity: usize,

    core: Arc<PackerCore<T, R>>,
}

impl<T, R> Default for Packer<T, R>
where
    T: 'static,
    R: 'static,
{
    fn default() -> Self {
        Self::new(DEFAULT_QUEUE_CAPACITY)
    }
}

impl<T, R> Packer<T, R>
where
    T: 'static,
    R: 'static,
{
    pub fn new(default_queue_capacity: usize) -> Self {
        Self {
            default_queue_capacity,
            core: Arc::new(PackerCore {
                queue: Mutex::new(Vec::with_capacity(default_queue_capacity)),
            }),
        }
    }

    pub fn append(&self, data: T, notifier: Option<oneshot::Sender<R>>) {
        self.core.queue.lock().push(Item { data, notifier });
    }

    pub fn package(&self) -> Vec<Item<T, R>> {
        let mut queue = self.core.queue.lock();
        let mut package = Vec::with_capacity(self.default_queue_capacity);
        std::mem::swap(&mut package, &mut (*queue));
        package
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<Packer<(), ()>>();
    }
}
