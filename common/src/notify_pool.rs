use std::collections::hash_map::{DefaultHasher, Entry, HashMap};
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

pub struct NotifyPoolCore<I, R>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<Mutex<HashMap<I, oneshot::Sender<R>>>>,
}

impl<I, R> Clone for NotifyPoolCore<I, R>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I, R> Default for NotifyPoolCore<I, R>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub struct NotifyPool<I, R>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
{
    shards: u16,
    buckets: HashMap<u16, NotifyPoolCore<I, R>>,
}

impl<I, R> Clone for NotifyPool<I, R>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
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
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
{
    #[allow(clippy::let_and_return)]
    pub fn new(shards: u16) -> Self {
        let mut buckets = HashMap::default();
        for i in 0..shards {
            buckets.insert(i, NotifyPoolCore::default());
        }
        let pool = Self { shards, buckets };
        #[cfg(feature = "trace-notify-pool")]
        {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                pool_clone.trace().await;
            });
        }
        pool
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

#[cfg(feature = "trace-notify-pool")]
#[derive(Default, Debug)]
struct TraceOutput<I>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
{
    total: usize,
    ids: Vec<I>,
}

#[cfg(feature = "trace-notify-pool")]
impl<I, R> NotifyPool<I, R>
where
    I: Eq + Hash + Copy + Clone + Send + Sync + Debug + Display + 'static,
    R: Send + Sync + 'static,
{
    async fn trace(&self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;

            let mut output = TraceOutput {
                total: 0,
                ids: vec![],
            };

            for (_, bucket) in self.buckets.iter() {
                let guard = bucket.inner.lock();
                for (id, _) in guard.iter() {
                    output.total += 1;
                    output.ids.push(*id);
                }
            }
            tracing::trace!("notofy pool: {:?}", output);
        }
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
