use bytes::Bytes;

use crate::lsm_tree::utils::{full_key, raw_value, value, IterRef, KeyComparator, Skiplist};

#[derive(Clone)]
pub struct Comparator;

impl KeyComparator for Comparator {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> std::cmp::Ordering {
        lhs.cmp(rhs)
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs.len() == rhs.len() && lhs[..lhs.len() - 8] == rhs[..rhs.len() - 8]
    }
}

#[derive(Clone)]
pub struct Memtable {
    inner: Skiplist<Comparator>,
    capacity: usize,
}

impl Memtable {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Skiplist::with_capacity(Comparator, capacity as u32),
            capacity,
        }
    }

    pub fn put(&self, key: &Bytes, value: Option<&Bytes>, timestamp: u64) {
        let key = full_key(key, timestamp);
        self.inner.put(key, raw_value(value.map(|v| &v[..])));
    }

    pub fn get(&self, key: &Bytes, timestamp: u64) -> Option<Bytes> {
        let raw = self.get_raw(key, timestamp)?;
        value(&raw).map(Bytes::copy_from_slice)
    }

    pub fn get_raw(&self, key: &Bytes, timestamp: u64) -> Option<Bytes> {
        let key = full_key(key, timestamp);
        self.inner.get(&key).cloned()
    }

    pub fn mem_remain(&self) -> usize {
        self.capacity - self.inner.mem_size() as usize
    }

    pub fn mem_size(&self) -> usize {
        self.inner.mem_size() as usize
    }

    pub(in crate::lsm_tree) fn iter(&self) -> IterRef<Skiplist<Comparator>, Comparator> {
        self.inner.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn unwrap(self) -> Skiplist<Comparator> {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future;
    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use super::*;
    use crate::lsm_tree::iterator::{Iterator, MemtableIterator, Seek};
    use crate::lsm_tree::DEFAULT_MEMTABLE_SIZE;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<Memtable>()
    }

    #[tokio::test]
    async fn test_concurrent_put() {
        // Insert multiple kvs out of order concurrently.
        // Then iterate from start to end to check if memtable is complete and ordered.
        // Then check if get returns correct results on a timestamp while inserting kvs
        // concurrently.
        let memtable = Memtable::new(DEFAULT_MEMTABLE_SIZE);
        let futures = (1..=10000)
            .map(|i| {
                let memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(&key(i), Some(&value(i)), i * 4);
                    memtable_clone.put(&key(i), None, i * 4 + 1);
                    memtable_clone.put(&key(i), Some(&value(i)), i * 4 + 2);
                }
            })
            .collect_vec();
        future::join_all(futures).await;
        let mut iter = MemtableIterator::new(&memtable, u64::MAX);
        iter.seek(Seek::First).await.unwrap();
        for i in 1..=10000 {
            assert_eq!(iter.key(), &key(i));
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        let put_futures = (1..=10000)
            .map(|i| {
                let memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(&key(i), Some(&value(i)), i * 4 + 3);
                }
            })
            .collect_vec();
        let get_futures = (1..=10000)
            .map(|i| {
                let memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    let v = memtable_clone.get(&key(i), i * 4 + 2);
                    assert_eq!(v, Some(value(i)));
                }
            })
            .collect_vec();
        let put_future = future::join_all(put_futures);
        let get_future = future::join_all(get_futures);
        future::join(put_future, get_future).await;
    }

    #[tokio::test]
    async fn test_concurrent_iter() {
        // Insert multiple kvs and create an iterator on the newest timestamp.
        // Then iterate from start to end and check results while inserting kvs concurrently.
        let memtable = Memtable::new(DEFAULT_MEMTABLE_SIZE * 4);
        for i in 1..=10000 {
            memtable.put(&key(i), Some(&value(i)), i * 3);
            memtable.put(&key(i), None, i * 3 + 1);
            memtable.put(&key(i), Some(&value(i)), i * 3 + 2);
        }

        let memtable_clone = memtable.clone();
        let assert_forward = async move {
            let mut iter = MemtableIterator::new(&memtable_clone, 39999);
            iter.seek(Seek::First).await.unwrap();
            for i in 1..=10000 {
                assert_eq!(iter.key(), &key(i));
                iter.next().await.unwrap();
            }
            assert!(!iter.is_valid());
        };
        let memtable_clone = memtable.clone();
        let assert_backward = async move {
            let mut iter = MemtableIterator::new(&memtable_clone, 39999);
            iter.seek(Seek::Last).await.unwrap();
            for i in (1..=10000).rev() {
                assert_eq!(iter.key(), &key(i));
                iter.prev().await.unwrap();
            }
            assert!(!iter.is_valid());
        };
        let put_futures = (1..=10000)
            .map(|i| {
                let memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(&key(i), Some(&value(i)), i + 40000);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(&key(i), Some(&value(i)), i + 50000);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(&key(i), Some(&value(i)), i + 60000);
                }
            })
            .collect_vec();
        future::join(
            future::join_all(put_futures),
            future::join(assert_forward, assert_backward),
        )
        .await;
    }

    fn key(i: u64) -> Bytes {
        Bytes::from(format!("k{:08}", i))
    }

    fn value(i: u64) -> Bytes {
        Bytes::from(format!("v{:08}", i))
    }
}
