use std::cmp::Ordering;

use async_trait::async_trait;

use super::{BoxedIterator, Iterator, Seek};
use crate::utils::compare_full_key;
use crate::Result;

pub struct ConcatIterator {
    /// Iterators to concat.
    iters: Vec<BoxedIterator>,
    /// Current iterator index.
    ///
    /// Note: If [`ConcatIterator`] is valid, current iterator must be valid, too.
    offset: usize,
}

impl ConcatIterator {
    /// Note: Input iterators must be in ASC order.
    pub fn new(iters: Vec<BoxedIterator>) -> Self {
        Self {
            iters,
            offset: usize::MAX,
        }
    }

    /// Invalidate current state after reaching a invalid state.
    fn invalid(&mut self) {
        self.offset = self.iters.len()
    }

    /// Move to the next entry.
    ///
    /// Note: Ensure that the current state is valid.
    async fn next_inner(&mut self) -> Result<()> {
        self.iters[self.offset].next().await?;
        match self.iters[self.offset].is_valid() {
            true => Ok(()),
            false => {
                if self.offset + 1 == self.iters.len() {
                    self.invalid();
                    Ok(())
                } else {
                    self.offset += 1;
                    self.iters[self.offset].seek(Seek::First).await?;
                    Ok(())
                }
            }
        }
    }

    /// Move to the previous entry.
    ///
    /// Note: Ensure that the current state is valid.
    async fn prev_inner(&mut self) -> Result<()> {
        self.iters[self.offset].prev().await?;
        match self.iters[self.offset].is_valid() {
            true => Ok(()),
            false => {
                if self.offset == 0 {
                    self.invalid();
                    Ok(())
                } else {
                    self.offset -= 1;
                    self.iters[self.offset].seek(Seek::Last).await?;
                    Ok(())
                }
            }
        }
    }

    /// Move backward until reach the first key that equals or smaller than the given `key`.
    async fn prev_until_key(&mut self, key: &[u8]) -> Result<()> {
        while self.is_valid() && compare_full_key(self.key(), key) == Ordering::Greater {
            self.prev_inner().await?;
        }
        Ok(())
    }

    async fn binary_seek(&mut self, key: &[u8]) -> Result<()> {
        let offset = self.binary_seek_inner(key).await?;
        if offset >= self.iters.len() {
            self.invalid();
            return Ok(());
        }
        self.iters[offset].seek(Seek::RandomForward(key)).await?;
        if self.iters[offset].is_valid() {
            self.offset = offset;
        } else {
            // Move to the first entry of the next inner iter.
            self.offset = offset + 1;
            if self.offset < self.iters.len() {
                self.iters[self.offset].seek(Seek::First).await?;
            } else {
                // No more valid entry, set invalid state.
                self.invalid()
            }
        }
        Ok(())
    }

    async fn binary_seek_inner(&mut self, key: &[u8]) -> Result<usize> {
        let mut size = self.iters.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            use std::cmp::Ordering::*;
            let mid = left + size / 2;
            let iter = &mut self.iters[mid];
            iter.seek(Seek::RandomForward(key)).await?;
            let cmp = if iter.is_valid() {
                compare_full_key(iter.key(), key)
            } else {
                Less
            };
            match cmp {
                Less => left = mid + 1,
                Equal => return Ok(mid),
                Greater => right = mid,
            }
            size = right - left;
        }
        Ok(left.saturating_sub(1))
    }
}

#[async_trait]
impl Iterator for ConcatIterator {
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
        self.iters[self.offset].key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.iters[self.offset].value()
    }

    fn is_valid(&self) -> bool {
        self.offset < self.iters.len()
    }

    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<bool> {
        let found = match seek {
            Seek::First => {
                self.offset = 0;
                self.iters[self.offset].seek(Seek::First).await?;
                self.is_valid()
            }
            Seek::Last => {
                self.offset = self.iters.len() - 1;
                self.iters[self.offset].seek(Seek::Last).await?;
                self.is_valid()
            }
            Seek::RandomForward(key) => {
                self.binary_seek(key).await?;
                self.is_valid() && self.key() == key
            }
            Seek::RandomBackward(key) => {
                self.binary_seek(key).await?;
                if !self.is_valid() {
                    self.seek(Seek::Last).await?;
                }
                self.prev_until_key(key).await?;
                self.is_valid() && self.key() == key
            }
        };
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use bytes::Bytes;
    use test_log::test;

    use super::*;
    use crate::components::{Block, BlockBuilder, BlockBuilderOptions};
    use crate::iterator::BlockIterator;
    use crate::utils::full_key;
    pub struct AsyncBlockIterator(BlockIterator);

    impl AsyncBlockIterator {
        pub fn new(block: Arc<Block>) -> Self {
            AsyncBlockIterator(BlockIterator::new(block))
        }
    }
    #[async_trait]
    impl Iterator for AsyncBlockIterator {
        async fn next(&mut self) -> Result<()> {
            self.0.next()
        }

        async fn prev(&mut self) -> Result<()> {
            self.0.prev()
        }

        fn key(&self) -> &[u8] {
            self.0.key()
        }

        fn value(&self) -> &[u8] {
            self.0.value()
        }

        fn is_valid(&self) -> bool {
            self.0.is_valid()
        }

        async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<bool> {
            self.0.seek(seek)
        }
    }

    fn build_iterator_for_test() -> ConcatIterator {
        ConcatIterator::new(vec![
            Box::new(AsyncBlockIterator::new(build_block_for_test(1..=3))),
            Box::new(AsyncBlockIterator::new(build_block_for_test(5..=7))),
            Box::new(AsyncBlockIterator::new(build_block_for_test(9..=11))),
        ])
    }

    fn build_block_for_test(range: RangeInclusive<usize>) -> Arc<Block> {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        for i in range {
            builder.add(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64),
                &Bytes::from(format!("v{:02}", i)),
            );
        }
        let buf = builder.build();
        Arc::new(Block::decode(buf).unwrap())
    }

    #[test(tokio::test)]
    async fn test_seek_first() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::First).await.unwrap();
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
    }

    #[test(tokio::test)]
    async fn test_seek_last() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(&full_key(b"k11", 11)[..], it.key());
    }

    #[test(tokio::test)]
    async fn test_seek_none_front() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomForward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k01", 1)[..], it.key());

        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomBackward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert!(!it.is_valid());
    }

    #[test(tokio::test)]
    async fn test_seek_none_back() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomForward(&full_key(b"k12", 12)[..]))
            .await
            .unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomBackward(&full_key(b"k12", 12)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k11", 11)[..], it.key());
    }

    #[test(tokio::test)]
    async fn bi_direction_seek() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomForward(&full_key(b"k04", 4)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 5).as_bytes(), 5)[..], it.key());

        it.seek(Seek::RandomBackward(&full_key(b"k04", 4)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 3).as_bytes(), 3)[..], it.key());
    }

    #[test(tokio::test)]
    async fn test_forward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(Seek::First).await.unwrap();
        for i in (1..=3).chain(5..=7).chain(9..=11) {
            assert!(it.is_valid());
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                it.key()
            );
            assert_eq!(format!("v{:02}", i).as_bytes(), it.value());
            it.next().await.unwrap();
        }
        assert!(!it.is_valid())
    }

    #[test(tokio::test)]
    async fn test_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(Seek::Last).await.unwrap();
        for i in (1..=3).chain(5..=7).chain(9..=11).rev() {
            assert!(it.is_valid());
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                it.key()
            );
            assert_eq!(format!("v{:02}", i).as_bytes(), it.value());
            it.prev().await.unwrap();
        }
        assert!(!it.is_valid())
    }

    #[test(tokio::test)]
    async fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(Seek::RandomForward(&full_key(b"k06", 6)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 6).as_bytes(), 6)[..], it.key());

        it.prev().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 5).as_bytes(), 5)[..], it.key());

        it.prev().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 3).as_bytes(), 3)[..], it.key());

        it.next().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 5).as_bytes(), 5)[..], it.key());

        it.next().await.unwrap();
        assert_eq!(&full_key(format!("k{:02}", 6).as_bytes(), 6)[..], it.key());
    }
}
