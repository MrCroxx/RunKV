use std::collections::hash_map::{DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[must_use = "if unused the RwLock will immediately unlock"]
pub struct ShardedHashMapRwLockReadGuard<'g, K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    inner: RwLockReadGuard<'g, HashMap<K, V>>,
    key: &'g K,
}

impl<'g, K, V> ShardedHashMapRwLockReadGuard<'g, K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    pub fn get(&self) -> Option<&'_ V> {
        self.inner.get(self.key)
    }
}

#[must_use = "if unused the RwLock will immediately unlock"]
pub struct ShardedHashMapRwLockWriteGuard<'g, K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    inner: RwLockWriteGuard<'g, HashMap<K, V>>,
    key: &'g K,
}

impl<'g, K, V> ShardedHashMapRwLockWriteGuard<'g, K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    pub fn get(&self) -> Option<&'_ V> {
        self.inner.get(self.key)
    }

    pub fn get_mut(&mut self) -> Option<&'_ mut V> {
        self.inner.get_mut(self.key)
    }
}

pub struct ShardedHashMapCore<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    inner: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> Clone for ShardedHashMapCore<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> Default for ShardedHashMapCore<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub struct ShardedHashMap<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    shards: u16,
    buckets: HashMap<u16, ShardedHashMapCore<K, V>>,
}

impl<K, V> Default for ShardedHashMap<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    fn default() -> Self {
        Self::new(64)
    }
}

impl<K, V> Clone for ShardedHashMap<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    fn clone(&self) -> Self {
        Self {
            shards: self.shards,
            buckets: self.buckets.clone(),
        }
    }
}

impl<K, V> ShardedHashMap<K, V>
where
    K: Eq + Hash + Copy + Clone + 'static,
    V: 'static,
{
    pub fn new(shards: u16) -> Self {
        let mut buckets = HashMap::default();
        for i in 0..shards {
            buckets.insert(i, ShardedHashMapCore::default());
        }
        Self { shards, buckets }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = (hash % self.shards as u64) as u16;

        let bucket = self.buckets.get(&shard).unwrap();

        bucket.inner.write().insert(key, value)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = (hash % self.shards as u64) as u16;

        let bucket = self.buckets.get(&shard).unwrap();

        bucket.inner.write().remove(key)
    }

    pub fn read<'a>(&'a self, key: &'a K) -> ShardedHashMapRwLockReadGuard<'_, K, V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = (hash % self.shards as u64) as u16;

        let bucket = self.buckets.get(&shard).unwrap();

        let inner = bucket.inner.read();

        ShardedHashMapRwLockReadGuard { inner, key }
    }

    pub fn write<'a>(&'a self, key: &'a K) -> ShardedHashMapRwLockWriteGuard<'_, K, V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = (hash % self.shards as u64) as u16;

        let bucket = self.buckets.get(&shard).unwrap();

        let inner = bucket.inner.write();

        ShardedHashMapRwLockWriteGuard { inner, key }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use test_log::test;

    use super::*;
    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<ShardedHashMap<u64, u64>>();
    }

    #[test]
    fn test_concurrent_put_get() {
        const CONCURRENCY: u64 = 1024;
        const SHARDS: u16 = 64;

        let map = ShardedHashMap::new(SHARDS);

        let job = |map: ShardedHashMap<u64, u64>, i: u64, total: u64| {
            {
                assert_eq!(map.insert(i, i), None);
            }
            {
                let read = map.read(&i);
                assert_eq!(read.get(), Some(&i));
            }
            {
                let mut write = map.write(&i);
                assert_eq!(write.get(), Some(&i));
                *write.get_mut().unwrap() += total;
                assert_eq!(write.get(), Some(&(i + total)));
            }
            {
                let read = map.read(&i);
                assert_eq!(read.get(), Some(&(i + total)));
            }
        };

        let handles = (0..CONCURRENCY)
            .into_iter()
            .map(|i| {
                let map_clone = map.clone();
                std::thread::spawn(move || job(map_clone, i, CONCURRENCY))
            })
            .collect_vec();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
