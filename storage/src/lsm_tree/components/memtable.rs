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
}

impl Memtable {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Skiplist::with_capacity(Comparator, capacity as u32),
        }
    }

    pub fn put(&mut self, key: &[u8], value: Option<&[u8]>, timestamp: u64) {
        let key = full_key(key, timestamp);
        self.inner.put(key, raw_value(value));
    }

    pub fn get(&self, key: &[u8], timestamp: u64) -> Option<&[u8]> {
        let key = full_key(key, timestamp);
        let raw = self.inner.get(&key).map(|k| &k[..])?;
        value(raw)
    }

    pub(in crate::lsm_tree) fn iter(&self) -> IterRef<Skiplist<Comparator>, Comparator> {
        self.inner.iter()
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
                let mut memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(
                        format!("k{:08}", i).as_bytes(),
                        Some(format!("v{:08}", i).as_bytes()),
                        i * 4,
                    );
                    memtable_clone.put(format!("k{:08}", i).as_bytes(), None, i * 4 + 1);
                    memtable_clone.put(
                        format!("k{:08}", i).as_bytes(),
                        Some(format!("v{:08}", i).as_bytes()),
                        i * 4 + 2,
                    );
                }
            })
            .collect_vec();
        future::join_all(futures).await;
        let mut iter = MemtableIterator::new(&memtable, u64::MAX);
        iter.seek(Seek::First).await.unwrap();
        for i in 1..=10000 {
            assert_eq!(iter.key(), format!("k{:08}", i).as_bytes());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        let put_futures = (1..=10000)
            .map(|i| {
                let mut memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(
                        format!("k{:08}", i).as_bytes(),
                        Some(format!("v{:08}", i).as_bytes()),
                        i * 4 + 3,
                    );
                }
            })
            .collect_vec();
        let get_futures = (1..=10000)
            .map(|i| {
                let memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    let v = memtable_clone.get(format!("k{:08}", i).as_bytes(), i * 4 + 2);
                    assert_eq!(v, Some(format!("v{:08}", i).as_bytes()));
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
        let mut memtable = Memtable::new(DEFAULT_MEMTABLE_SIZE * 4);
        for i in 1..=10000 {
            memtable.put(
                format!("k{:08}", i).as_bytes(),
                Some(format!("v{:08}", i).as_bytes()),
                i * 3,
            );
            memtable.put(format!("k{:08}", i).as_bytes(), None, i * 3 + 1);
            memtable.put(
                format!("k{:08}", i).as_bytes(),
                Some(format!("v{:08}", i).as_bytes()),
                i * 3 + 2,
            );
        }

        let memtable_clone = memtable.clone();
        let assert_forward = async move {
            let mut iter = MemtableIterator::new(&memtable_clone, 39999);
            iter.seek(Seek::First).await.unwrap();
            for i in 1..=10000 {
                assert_eq!(iter.key(), format!("k{:08}", i).as_bytes());
                iter.next().await.unwrap();
            }
            assert!(!iter.is_valid());
        };
        let memtable_clone = memtable.clone();
        let assert_backward = async move {
            let mut iter = MemtableIterator::new(&memtable_clone, 39999);
            iter.seek(Seek::Last).await.unwrap();
            for i in (1..=10000).rev() {
                assert_eq!(iter.key(), format!("k{:08}", i).as_bytes());
                iter.prev().await.unwrap();
            }
            assert!(!iter.is_valid());
        };
        let put_futures = (1..=10000)
            .map(|i| {
                let mut memtable_clone = memtable.clone();
                async move {
                    let mut rng = thread_rng();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(
                        format!("k{:08}", i).as_bytes(),
                        Some(format!("v{:08}", i).as_bytes()),
                        i + 40000,
                    );
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(
                        format!("k{:08}", i).as_bytes(),
                        Some(format!("v{:08}", i).as_bytes()),
                        i + 50000,
                    );
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..500))).await;
                    memtable_clone.put(
                        format!("k{:08}", i).as_bytes(),
                        Some(format!("v{:08}", i).as_bytes()),
                        i + 60000,
                    );
                }
            })
            .collect_vec();
        future::join(
            future::join_all(put_futures),
            future::join(assert_forward, assert_backward),
        )
        .await;
    }
}
