use std::cmp::Reverse;
use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::collections::LinkedList;

use async_trait::async_trait;

use super::{Iterator, Seek};
use crate::{BoxedIterator, Result};

#[derive(PartialEq, Debug)]
enum Direction {
    Forward,
    Backward,
}

pub struct MergeIterator {
    /// Current direction.
    direction: Direction,
    /// Invalid iterators.
    iters: LinkedList<BoxedIterator>,
    /// Min heap.
    ///
    /// `min_heap` is ensured not empty when valid and forward.
    min_heap: BinaryHeap<Reverse<BoxedIterator>>,
    /// Max heap.
    ///
    /// `max_heap` is ensured not empty when valid and backward.
    max_heap: BinaryHeap<BoxedIterator>,
}

impl MergeIterator {
    pub fn new(iters: Vec<BoxedIterator>) -> Self {
        let len = iters.len();
        Self {
            direction: Direction::Forward,
            iters: LinkedList::from_iter(iters.into_iter()),
            min_heap: BinaryHeap::with_capacity(len),
            max_heap: BinaryHeap::with_capacity(len),
        }
    }

    async fn may_rebuild_heap(&mut self, direction: Direction) -> Result<()> {
        if self.direction == direction {
            return Ok(());
        }
        let key = self.key().to_vec();
        self.direction = direction;
        self.iters.extend(self.min_heap.drain().map(|r| r.0));
        self.iters.extend(self.max_heap.drain());
        for iter in self.iters.iter_mut() {
            match self.direction {
                Direction::Forward => iter.seek(Seek::RandomForward(&key)).await?,
                Direction::Backward => iter.seek(Seek::RandomBackward(&key)).await?,
            }
        }
        match self.direction {
            Direction::Forward => {
                self.min_heap
                    .extend(self.iters.drain_filter(|iter| iter.is_valid()).map(Reverse));
            }
            Direction::Backward => self
                .max_heap
                .extend(self.iters.drain_filter(|iter| iter.is_valid())),
        }
        Ok(())
    }

    async fn next_inner(&mut self) -> Result<()> {
        self.may_rebuild_heap(Direction::Forward).await?;
        let mut iter = self.min_heap.peek_mut().unwrap();
        iter.0.next().await?;
        if !iter.0.is_valid() {
            let iter = PeekMut::pop(iter);
            self.iters.push_back(iter.0);
        }
        Ok(())
    }

    async fn prev_inner(&mut self) -> Result<()> {
        self.may_rebuild_heap(Direction::Backward).await?;
        let mut iter = self.max_heap.peek_mut().unwrap();
        iter.prev().await?;
        if !iter.is_valid() {
            let iter = PeekMut::pop(iter);
            self.iters.push_back(iter);
        }
        Ok(())
    }
}

#[async_trait]
impl Iterator for MergeIterator {
    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.next_inner().await
    }

    async fn prev(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.prev_inner().await
    }

    fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        match self.direction {
            Direction::Forward => self.min_heap.peek().unwrap().0.key(),
            Direction::Backward => self.max_heap.peek().unwrap().key(),
        }
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        match self.direction {
            Direction::Forward => self.min_heap.peek().unwrap().0.value(),
            Direction::Backward => self.max_heap.peek().unwrap().value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.direction {
            Direction::Forward => !self.min_heap.is_empty(),
            Direction::Backward => !self.max_heap.is_empty(),
        }
    }

    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<()> {
        match seek {
            Seek::First => {
                self.direction = Direction::Forward;
                self.iters.extend(self.min_heap.drain().map(|r| r.0));
                self.iters.extend(self.max_heap.drain());
                while !self.iters.is_empty() {
                    let mut iter = self.iters.pop_back().unwrap();
                    iter.seek(Seek::First).await?;
                    self.min_heap.push(Reverse(iter));
                }
            }
            Seek::Last => {
                self.direction = Direction::Backward;
                self.iters.extend(self.min_heap.drain().map(|r| r.0));
                self.iters.extend(self.max_heap.drain());
                while !self.iters.is_empty() {
                    let mut iter = self.iters.pop_back().unwrap();
                    iter.seek(Seek::Last).await?;
                    self.max_heap.push(iter);
                }
            }
            Seek::RandomForward(key) => {
                self.direction = Direction::Forward;
                self.iters.extend(self.min_heap.drain().map(|r| r.0));
                self.iters.extend(self.max_heap.drain());
                while !self.iters.is_empty() {
                    let mut iter = self.iters.pop_back().unwrap();
                    iter.seek(Seek::RandomForward(key)).await?;
                    if iter.is_valid() {
                        self.min_heap.push(Reverse(iter));
                    }
                }
            }
            Seek::RandomBackward(key) => {
                self.direction = Direction::Backward;
                self.iters.extend(self.min_heap.drain().map(|r| r.0));
                self.iters.extend(self.max_heap.drain());
                while !self.iters.is_empty() {
                    let mut iter = self.iters.pop_back().unwrap();
                    iter.seek(Seek::RandomBackward(key)).await?;
                    if iter.is_valid() {
                        self.max_heap.push(iter);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::{full_key, Block, BlockBuilder, BlockBuilderOptions, BlockIterator};

    fn build_iterator_for_test() -> MergeIterator {
        MergeIterator::new(vec![
            Box::new(BlockIterator::new(build_block_for_test(&[1, 5, 9]))),
            Box::new(BlockIterator::new(build_block_for_test(&[2, 6, 10]))),
            Box::new(BlockIterator::new(build_block_for_test(&[3, 7, 11]))),
        ])
    }

    fn build_block_for_test(range: &[usize]) -> Arc<Block> {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        for i in range {
            builder.add(
                &full_key(format!("k{:02}", i).as_bytes(), *i as u64),
                &Bytes::from(format!("v{:02}", i)),
            );
        }
        let buf = builder.build();
        Arc::new(Block::decode(buf).unwrap())
    }

    #[tokio::test]
    async fn test_seek_first() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::First).await.unwrap();
        assert!(mi.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], mi.key());
        assert_eq!(b"v01", mi.value());
    }

    #[tokio::test]
    async fn test_seek_last() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::Last).await.unwrap();
        assert!(mi.is_valid());
        assert_eq!(&full_key(b"k11", 11)[..], mi.key());
        assert_eq!(b"v11", mi.value());
    }

    #[tokio::test]
    async fn test_seek_random() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::RandomForward(&full_key(b"k06", 6)[..]))
            .await
            .unwrap();
        assert!(mi.is_valid());
        assert_eq!(&full_key(b"k06", 6)[..], mi.key());
        assert_eq!(b"v06", mi.value());
    }

    #[tokio::test]
    async fn test_seek_none_front() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::RandomForward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert!(mi.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], mi.key());
        assert_eq!(b"v01", mi.value());
    }

    #[tokio::test]
    async fn test_seek_none_middle() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::RandomForward(&full_key(b"k04", 4)[..]))
            .await
            .unwrap();
        assert!(mi.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], mi.key());
        assert_eq!(b"v05", mi.value());
    }

    #[tokio::test]
    async fn test_seek_none_back() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::RandomForward(&full_key(b"k12", 12)[..]))
            .await
            .unwrap();
        assert!(!mi.is_valid());
    }

    #[tokio::test]
    async fn test_forward_iterate() {
        let mut mi = build_iterator_for_test();

        mi.seek(Seek::First).await.unwrap();
        for i in (1..=3).chain(5..=7).chain(9..=11) {
            assert!(mi.is_valid());
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                mi.key()
            );
            assert_eq!(format!("v{:02}", i).as_bytes(), mi.value());
            mi.next().await.unwrap();
        }
        assert!(!mi.is_valid())
    }

    #[tokio::test]
    async fn test_backward_iterate() {
        let mut mi = build_iterator_for_test();

        mi.seek(Seek::Last).await.unwrap();
        for i in (1..=3).chain(5..=7).chain(9..=11).rev() {
            assert!(mi.is_valid());
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                mi.key()
            );
            assert_eq!(format!("v{:02}", i).as_bytes(), mi.value());
            mi.prev().await.unwrap();
        }
        assert!(!mi.is_valid())
    }

    #[tokio::test]
    async fn test_seek_forward_backward_iterate() {
        let mut mi = build_iterator_for_test();

        mi.seek(Seek::RandomForward(&full_key(b"k06", 6)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 6).as_bytes(), 6)[..], mi.key());

        mi.prev().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 5).as_bytes(), 5)[..], mi.key());

        mi.prev().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 3).as_bytes(), 3)[..], mi.key());

        mi.next().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 5).as_bytes(), 5)[..], mi.key());

        mi.next().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 6).as_bytes(), 6)[..], mi.key());
    }

    #[tokio::test]
    async fn bi_direction_seek() {
        let mut mi = build_iterator_for_test();
        mi.seek(Seek::RandomForward(&full_key(b"k04", 4)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 5).as_bytes(), 5)[..], mi.key());

        mi.seek(Seek::RandomBackward(&full_key(b"k04", 4)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 3).as_bytes(), 3)[..], mi.key());
    }
}
