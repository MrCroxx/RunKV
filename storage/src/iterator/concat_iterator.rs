use async_trait::async_trait;

use super::{BinarySeekableIterator, Iterator, Seek};
use crate::Result;

pub struct ConcatIterator {
    /// Iterators to concat.
    iters: Vec<Box<dyn Iterator>>,
    /// Current iterator index.
    ///
    /// Note: If [`ConcatIterator`] is valid, current iterator must be valid, too.
    offset: usize,
}

impl ConcatIterator {
    /// Note: Input iterators must be in ASC order.

    pub fn new(iters: Vec<Box<dyn Iterator>>) -> Self {
        Self { iters, offset: 0 }
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
                    self.iters[self.offset].seek(Seek::First).await
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
                    self.iters[self.offset].seek(Seek::Last).await
                }
            }
        }
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

    async fn seek<'s>(&mut self, position: Seek<'s>) -> Result<()> {
        match position {
            Seek::First => {
                self.offset = 0;
                self.iters[self.offset].seek(Seek::First).await
            }
            Seek::Last => {
                self.offset = self.iters.len() - 1;
                self.iters[self.offset].seek(Seek::Last).await
            }
            Seek::Random(key) => {
                self.binary_seek(key).await?;
                println!("offset: {}", self.offset);
                if self.offset >= self.iters.len() {
                    self.invalid();
                } else {
                    self.iters[self.offset].seek(Seek::Random(key)).await?;
                    if !self.iters[self.offset].is_valid() {
                        // Move to the first entry of the next inner iter.
                        self.offset += 1;
                        if self.offset < self.iters.len() {
                            self.iters[self.offset].seek(Seek::First).await?;
                        } else {
                            self.invalid();
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl BinarySeekableIterator for ConcatIterator {
    fn len(&self) -> usize {
        self.iters.len()
    }

    fn get_mut(&mut self, index: usize) -> &mut Box<dyn Iterator> {
        &mut self.iters[index]
    }

    fn set_offset(&mut self, index: usize) {
        self.offset = index;
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use super::*;
    use crate::test_utils::full_key;
    use crate::{Block, BlockBuilder, BlockBuilderOptions, BlockIterator};

    fn build_iterator_for_test() -> ConcatIterator {
        ConcatIterator::new(vec![
            Box::new(BlockIterator::new(build_block_for_test(1..=3))),
            Box::new(BlockIterator::new(build_block_for_test(5..=7))),
            Box::new(BlockIterator::new(build_block_for_test(9..=11))),
        ])
    }

    fn build_block_for_test(range: RangeInclusive<usize>) -> Arc<Block> {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        for i in range {
            builder.add(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                format!("v{:02}", i).as_bytes(),
            );
        }
        let buf = builder.build();
        Arc::new(Block::decode(buf).unwrap())
    }

    #[tokio::test]
    async fn test_seek_first() {
        let mut ci = build_iterator_for_test();
        ci.seek(Seek::First).await.unwrap();
        assert!(ci.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], ci.key());
        assert_eq!(b"v01", ci.value());
    }

    #[tokio::test]
    async fn test_seek_last() {
        let mut ci = build_iterator_for_test();
        ci.seek(Seek::Last).await.unwrap();
        assert!(ci.is_valid());
        assert_eq!(&full_key(b"k11", 11)[..], ci.key());
        assert_eq!(b"v11", ci.value());
    }

    #[tokio::test]
    async fn test_seek_random() {
        let mut ci = build_iterator_for_test();
        ci.seek(Seek::Random(&full_key(b"k06", 6)[..]))
            .await
            .unwrap();
        assert!(ci.is_valid());
        assert_eq!(&full_key(b"k06", 6)[..], ci.key());
        assert_eq!(b"v06", ci.value());
    }

    #[tokio::test]
    async fn test_seek_none_front() {
        let mut ci = build_iterator_for_test();
        ci.seek(Seek::Random(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert!(ci.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], ci.key());
        assert_eq!(b"v01", ci.value());
    }

    #[tokio::test]
    async fn test_seek_none_middle() {
        let mut ci = build_iterator_for_test();
        ci.seek(Seek::Random(&full_key(b"k04", 4)[..]))
            .await
            .unwrap();
        assert!(ci.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], ci.key());
        assert_eq!(b"v05", ci.value());
    }

    #[tokio::test]
    async fn test_seek_none_back() {
        let mut ci = build_iterator_for_test();
        ci.seek(Seek::Random(&full_key(b"k12", 12)[..]))
            .await
            .unwrap();
        assert!(!ci.is_valid());
    }

    #[tokio::test]
    async fn test_forward_iterate() {
        let mut ci = build_iterator_for_test();

        ci.seek(Seek::First).await.unwrap();
        for i in (1..=3).chain(5..=7).chain(9..=11) {
            assert!(ci.is_valid());
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                ci.key()
            );
            assert_eq!(format!("v{:02}", i).as_bytes(), ci.value());
            ci.next().await.unwrap();
        }
        assert!(!ci.is_valid())
    }

    #[tokio::test]
    async fn test_backward_iterate() {
        let mut ci = build_iterator_for_test();

        ci.seek(Seek::Last).await.unwrap();
        for i in (1..=3).chain(5..=7).chain(9..=11).rev() {
            assert!(ci.is_valid());
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                ci.key()
            );
            assert_eq!(format!("v{:02}", i).as_bytes(), ci.value());
            ci.prev().await.unwrap();
        }
        assert!(!ci.is_valid())
    }
}
