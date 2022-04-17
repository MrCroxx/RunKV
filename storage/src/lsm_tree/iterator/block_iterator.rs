use std::cmp::Ordering;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::Seek;
use crate::components::{Block, KeyPrefix};
use crate::utils::compare_full_key;
use crate::Result;

/// [`BlockIterator`] is used to read kv pairs in a block.
pub struct BlockIterator {
    /// Block that iterates on.
    block: Arc<Block>,
    /// Current restart point index.
    restart_point_index: usize,
    /// Current offset.
    offset: usize,
    /// Current key.
    key: BytesMut,
    /// Current value.
    value: Bytes,
    /// Current entry len.
    entry_len: usize,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            offset: usize::MAX,
            restart_point_index: usize::MAX,
            key: BytesMut::default(),
            value: Bytes::default(),
            entry_len: 0,
        }
    }

    /// Invalidate current state after reaching a invalid state.
    fn invalid(&mut self) {
        self.offset = self.block.len();
        self.restart_point_index = self.block.restart_point_len();
        self.key.clear();
        self.value.clear();
        self.entry_len = 0;
    }

    /// Move to the next entry.
    ///
    /// Note: Ensure that the current state is valid.
    fn next_inner(&mut self) {
        let offset = self.offset + self.entry_len;
        if offset >= self.block.len() {
            self.invalid();
            return;
        }
        let prefix = self.decode_prefix_at(offset);
        self.key.truncate(prefix.overlap_len());
        self.key
            .extend_from_slice(self.block.slice(prefix.diff_key_range()));
        self.value = Bytes::copy_from_slice(self.block.slice(prefix.value_range()));
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        if self.restart_point_index + 1 < self.block.restart_point_len()
            && self.offset >= self.block.restart_point(self.restart_point_index + 1) as usize
        {
            self.restart_point_index += 1;
        }
    }

    /// Move forward until reach the first that equals or larger than the given `key`.
    fn next_until_key(&mut self, key: &[u8]) {
        while self.is_valid() && compare_full_key(&self.key[..], key) == Ordering::Less {
            self.next_inner();
        }
    }

    /// Move backward until reach the first key that equals or smaller than the given `key`.
    fn prev_until_key(&mut self, key: &[u8]) {
        while self.is_valid() && compare_full_key(&self.key[..], key) == Ordering::Greater {
            self.prev_inner();
        }
    }

    /// Move forward until the position reaches the previous position of the given `next_offset` or
    /// the last valid position if exists.
    fn next_until_prev(&mut self, offset: usize) {
        while self.offset + self.entry_len < std::cmp::min(self.block.len(), offset) {
            self.next_inner();
        }
    }

    /// Move to the previous entry.
    ///
    /// Note: Ensure that the current state is valid.
    fn prev_inner(&mut self) {
        if self.offset == 0 {
            self.invalid();
            return;
        }
        if self.block.restart_point(self.restart_point_index) as usize == self.offset {
            self.restart_point_index -= 1;
        }
        let origin_offset = self.offset;
        self.seek_restart_point_by_index(self.restart_point_index);
        self.next_until_prev(origin_offset);
    }

    /// Decode [`KeyPrefix`] at given offset.
    fn decode_prefix_at(&self, offset: usize) -> KeyPrefix {
        KeyPrefix::decode(
            &mut self
                .block
                .slice(offset..std::cmp::min(offset + KeyPrefix::max_len(), self.block.len())),
            offset,
        )
    }

    /// Search the restart point index that the given `key` belongs to.
    fn search_restart_point_index_by_key(&self, key: &[u8]) -> usize {
        self.block.search_restart_point_by(|probe| {
            let prefix = self.decode_prefix_at(*probe as usize);
            let probe_key = self.block.slice(prefix.diff_key_range());
            compare_full_key(probe_key, key)
        })
    }

    /// Seek to the restart point that the given `key` belongs to.
    fn seek_restart_point_by_key(&mut self, key: &[u8]) {
        let index = self.search_restart_point_index_by_key(key);
        self.seek_restart_point_by_index(index)
    }

    /// Seek to the restart point by given restart point index.
    fn seek_restart_point_by_index(&mut self, index: usize) {
        let offset = self.block.restart_point(index) as usize;
        let prefix = self.decode_prefix_at(offset);
        self.key = BytesMut::from(self.block.slice(prefix.diff_key_range()));
        self.value = Bytes::copy_from_slice(self.block.slice(prefix.value_range()));
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        self.restart_point_index = index;
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.next_inner();
        Ok(())
    }

    pub fn prev(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.prev_inner();
        Ok(())
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key[..]
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.value[..]
    }

    pub fn is_valid(&self) -> bool {
        self.offset < self.block.len()
    }

    pub fn seek(&mut self, seek: Seek) -> Result<bool> {
        let found = match seek {
            Seek::First => {
                self.seek_restart_point_by_index(0);
                self.is_valid()
            }
            Seek::Last => {
                self.seek_restart_point_by_index(self.block.restart_point_len() - 1);
                self.next_until_prev(self.block.len());
                self.is_valid()
            }
            Seek::RandomForward(key) => {
                self.seek_restart_point_by_key(key);
                self.next_until_key(key);
                self.is_valid() && self.key() == key
            }
            Seek::RandomBackward(key) => {
                self.seek_restart_point_by_key(key);
                self.next_until_key(key);
                if !self.is_valid() {
                    self.seek(Seek::Last)?;
                }
                self.prev_until_key(key);
                self.is_valid() && self.key() == key
            }
        };
        Ok(found)
    }
}

#[cfg(test)]
pub mod tests {
    use async_trait::async_trait;
    use test_log::test;

    use super::*;
    use crate::components::{BlockBuilder, BlockBuilderOptions};
    use crate::iterator::Iterator;
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

    fn build_iterator_for_test() -> BlockIterator {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k01", 1), b"v01");
        builder.add(&full_key(b"k02", 2), b"v02");
        builder.add(&full_key(b"k04", 4), b"v04");
        builder.add(&full_key(b"k05", 5), b"v05");
        let buf = builder.build();
        BlockIterator::new(Arc::new(Block::decode(buf).unwrap()))
    }

    #[test]
    fn test_seek_first() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::First).unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());
    }

    #[test]
    fn test_seek_last() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::Last).unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn test_seek_none_front() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomForward(&full_key(b"k00", 0)[..]))
            .unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomBackward(&full_key(b"k00", 0)[..]))
            .unwrap();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_none_back() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomForward(&full_key(b"k06", 6)[..]))
            .unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomBackward(&full_key(b"k06", 6)[..]))
            .unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn bi_direction_seek() {
        let mut it = build_iterator_for_test();
        it.seek(Seek::RandomForward(&full_key(b"k03", 3)[..]))
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());

        it.seek(Seek::RandomBackward(&full_key(b"k03", 3)[..]))
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 2).as_bytes(), 2)[..], it.key());
    }

    #[test]
    fn test_forward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(Seek::First).unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        it.next().unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
        assert_eq!(b"v02", it.value());

        it.next().unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
        assert_eq!(b"v04", it.value());

        it.next().unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());

        it.next().unwrap();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(Seek::Last).unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());

        it.prev().unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
        assert_eq!(b"v04", it.value());

        it.prev().unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
        assert_eq!(b"v02", it.value());

        it.prev().unwrap();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        it.prev().unwrap();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(Seek::RandomForward(&full_key(b"k03", 3)[..]))
            .unwrap();
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());

        it.prev().unwrap();
        assert_eq!(&full_key(format!("k{:02}", 2).as_bytes(), 2)[..], it.key());

        it.next().unwrap();
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());
    }
}
