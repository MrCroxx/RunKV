use std::collections::hash_map::{DefaultHasher, Entry, HashMap};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

pub struct NotifyPoolCore<I, R>
where
    I: Eq + Hash + Copy + Clone + Display,
{
    inner: Arc<Mutex<HashMap<I, oneshot::Sender<R>>>>,
}

impl<I, R> Clone for NotifyPoolCore<I, R>
where
    I: Eq + Hash + Copy + Clone + Display,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I, R> Default for NotifyPoolCore<I, R>
where
    I: Eq + Hash + Copy + Clone + Display,
{
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub struct NotifyPool<I, R>
where
    I: Eq + Hash + Copy + Clone + Display,
{
    shards: u16,
    buckets: HashMap<u16, NotifyPoolCore<I, R>>,
}

impl<I, R> Clone for NotifyPool<I, R>
where
    I: Eq + Hash + Copy + Clone + Display,
{
    fn clone(&self) -> Self {
        Self {
            shards: self.shards,
            buckets: self.buckets.clone(),
        }
    }
}

impl<I, R> NotifyPool<I, R>
where
    I: Eq + Hash + Copy + Clone + Display,
{
    pub fn new(shards: u16) -> Self {
        let mut buckets = HashMap::default();
        for i in 0..shards {
            buckets.insert(i, NotifyPoolCore::default());
        }
        Self { shards, buckets }
    }

    pub fn register(&self, id: I) -> anyhow::Result<oneshot::Receiver<R>> {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = (hash % self.shards as u64) as u16;

        let (tx, rx) = oneshot::channel();

        let bucket = self.buckets.get(&shard).unwrap();

        match bucket.inner.lock().entry(id) {
            Entry::Occupied(_) => return Err(anyhow::anyhow!("id {} already exists", id)),
            Entry::Vacant(v) => {
                v.insert(tx);
            }
        }
        Ok(rx)
    }

    pub fn notify(&self, id: I, result: R) -> anyhow::Result<()> {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = (hash % self.shards as u64) as u16;

        let bucket = self.buckets.get(&shard).unwrap();

        let tx = bucket
            .inner
            .lock()
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
    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<NotifyPool<u64, ()>>();
    }
}
