use std::cmp::Ordering;

use async_trait::async_trait;
use bytes::Bytes;

use super::{Iterator, Seek};
use crate::lsm_tree::utils::{full_key, IterRef, Skiplist};
use crate::{Comparator, Memtable, Result};
pub struct MemtableIterator {
    iter: IterRef<Skiplist<Comparator>, Comparator>,
    timestamp: u64,
}

impl MemtableIterator {
    pub fn new(memtable: &Memtable, timestamp: u64) -> Self {
        Self {
            iter: memtable.iter(),
            timestamp,
        }
    }

    /// Move backward until the position that the given `key` can be inserted in DESC order or EOF.
    fn prev_until_key(&mut self, key: &Bytes) {
        while self.is_valid() && self.key().cmp(key) == Ordering::Greater {
            self.iter.prev();
        }
    }
}

#[async_trait]
impl Iterator for MemtableIterator {
    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.iter.next();
        Ok(())
    }

    async fn prev(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.iter.prev();
        Ok(())
    }

    fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        self.iter.valid()
    }

    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<()> {
        match seek {
            Seek::First => self.iter.seek_to_first(),
            Seek::Last => self.iter.seek_to_last(),
            Seek::RandomForward(key) => self.iter.seek(&full_key(key, self.timestamp)),
            Seek::RandomBackward(key) => {
                let key = full_key(key, self.timestamp);
                self.iter.seek(&key);
                if !self.iter.valid() {
                    self.iter.seek_to_last();
                }
                self.prev_until_key(&key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::lsm_tree::DEFAULT_MEMTABLE_SIZE;

    fn build_memtable_for_test() -> Memtable {
        let mut memtable = Memtable::new(DEFAULT_MEMTABLE_SIZE);
        memtable.put(b"k04", Some(b"v04"), 1);
        memtable.put(b"k02", Some(b"v02"), 2);
        memtable.put(b"k05", Some(b"v05"), 3);
        memtable.put(b"k08", Some(b"v08"), 4);
        memtable.put(b"k01", Some(b"v01"), 5);
        memtable.put(b"k07", Some(b"v07"), 6);
        memtable
    }

    fn build_iterator_for_test(timestamp: u64) -> MemtableIterator {
        let memtable = build_memtable_for_test();
        MemtableIterator::new(&memtable, timestamp)
    }

    #[tokio::test]
    async fn test_seek_first() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::First).await.unwrap();
        assert_eq!(&full_key(b"k01", 5)[..], it.key());
    }

    #[tokio::test]
    async fn test_seek_last() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(&full_key(b"k08", 4)[..], it.key());
    }

    #[tokio::test]
    async fn test_seek_none_front() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomForward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k01", 5)[..], it.key());

        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomBackward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_seek_none_back() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomForward(&full_key(b"k09", 9)[..]))
            .await
            .unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomBackward(&full_key(b"k09", 9)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k08", 4)[..], it.key());
    }

    #[tokio::test]
    async fn bi_direction_seek() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomForward(&full_key(b"k03", u64::MAX)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k04", 1)[..], it.key());

        it.seek(Seek::RandomBackward(&full_key(b"k03", u64::MAX)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
    }

    #[tokio::test]
    async fn test_forward_iterate() {
        let mut it = build_iterator_for_test(u64::MAX);
        let m: HashMap<u64, u64> =
            HashMap::from_iter([(1, 5), (2, 2), (4, 1), (5, 3), (7, 6), (8, 4)].into_iter());
        it.seek(Seek::First).await.unwrap();
        for i in (1..=2).chain(4..=5).chain(7..=8) {
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), *m.get(&i).unwrap())[..],
                it.key()
            );
            it.next().await.unwrap();
        }
        assert!(!it.is_valid())
    }

    #[tokio::test]
    async fn test_backward_iterate() {
        let mut it = build_iterator_for_test(u64::MAX);
        let m: HashMap<u64, u64> =
            HashMap::from_iter([(1, 5), (2, 2), (4, 1), (5, 3), (7, 6), (8, 4)].into_iter());
        it.seek(Seek::Last).await.unwrap();
        for i in (1..=2).chain(4..=5).chain(7..=8).rev() {
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), *m.get(&i).unwrap())[..],
                it.key()
            );
            it.prev().await.unwrap();
        }
        assert!(!it.is_valid())
    }

    #[tokio::test]
    async fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test(u64::MAX);

        it.seek(Seek::RandomForward(&full_key(b"k03", u64::MAX)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k04", 1)[..], it.key());

        it.prev().await.unwrap();
        assert_eq!(&full_key(b"k02", 2)[..], it.key());

        it.next().await.unwrap();
        assert_eq!(&full_key(b"k04", 1)[..], it.key());
    }
}
