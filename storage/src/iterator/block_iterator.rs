use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use super::{Iterator, Seek};
use crate::{Block, KeyPrefix, Result};

/// [`BlockIterator`] is used to read kv pairs in a block.
pub struct BlockIterator {
    /// Block that iterates on.
    block: Arc<Block>,
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
            offset: 0,
            key: BytesMut::default(),
            value: Bytes::default(),
            entry_len: 0,
        }
    }
}

impl BlockIterator {
    /// Clear state after reaching a invalid state.
    fn clear(&mut self) {
        self.offset = self.block.len();
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
            self.clear();
            return;
        }
        let prefix = self.decode_prefix_at(offset);
        self.key.truncate(prefix.overlap_len());
        self.key
            .extend_from_slice(&self.block.slice(prefix.diff_key_range()));
        self.value = self.block.slice(prefix.value_range());
        self.offset = offset;
        self.entry_len = prefix.entry_len();
    }

    /// Move forward until the position that the given `key` can be inserted in order or EOF.
    fn next_until_key(&mut self, key: &[u8]) {
        while self.is_valid() && (&self.key[..]).cmp(key) == Ordering::Less {
            self.next_inner();
        }
    }

    /// Move forward until the position reaches the previous position of the given `next_offset` or
    /// the last valid position if exists.
    fn next_until_next_offset(&mut self, next_offset: usize) {
        while self.offset + self.entry_len < std::cmp::min(self.block.len(), next_offset) {
            self.next_inner()
        }
    }

    /// Move to the previous entry.
    ///
    /// Note: Ensure that the current state is valid.
    fn prev_inner(&mut self) {
        if self.offset == 0 {
            self.clear();
            return;
        }
        let origin_offset = self.offset;
        let mut restart_point_index = self.search_restart_point_index_by_key(self.key());
        if self.offset == self.block.restart_point(restart_point_index) as usize {
            restart_point_index -= 1;
        }
        self.seek_restart_point_by_index(restart_point_index);
        self.next_until_next_offset(origin_offset);
    }

    /// Decode [`KeyPrefix`] at given offset.
    fn decode_prefix_at(&self, offset: usize) -> KeyPrefix {
        KeyPrefix::decode(&mut &self.block.slice(offset..)[..], offset)
    }

    /// Search the restart point index that the given `key` belongs to.
    fn search_restart_point_index_by_key(&self, key: &[u8]) -> usize {
        self.block.search_restart_point_by(|probe| {
            let prefix = self.decode_prefix_at(*probe as usize);
            let probe_key = self.block.slice(prefix.diff_key_range());
            (&probe_key[..]).cmp(key)
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
        self.key = BytesMut::from(&self.block.slice(prefix.diff_key_range())[..]);
        self.value = self.block.slice(prefix.value_range());
        self.offset = offset;
        self.entry_len = prefix.entry_len();
    }
}

#[async_trait]
impl Iterator for BlockIterator {
    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.next_inner();
        Ok(())
    }

    async fn prev(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.prev_inner();
        Ok(())
    }

    fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key[..]
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.value[..]
    }

    fn is_valid(&self) -> bool {
        self.offset < self.block.len()
    }

    async fn seek<'s>(&mut self, position: Seek<'s>) -> Result<()> {
        match position {
            Seek::First => self.seek_restart_point_by_index(0),
            Seek::Last => {
                self.seek_restart_point_by_index(self.block.restart_point_len() - 1);
                self.next_until_next_offset(self.block.len())
            }
            Seek::Random(key) => {
                self.seek_restart_point_by_key(key);
                self.next_until_key(key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::full_key;
    use crate::{BlockBuilder, BlockBuilderOptions};

    fn build_block_for_test() -> Arc<Block> {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k1", 1), b"v1");
        builder.add(&full_key(b"k2", 2), b"v2");
        builder.add(&full_key(b"k3", 3), b"v3");
        builder.add(&full_key(b"k4", 4), b"v4");
        let buf = builder.build();
        Arc::new(Block::decode(buf).unwrap())
    }

    #[tokio::test]
    async fn test_seek_first() {
        let block = build_block_for_test();
        let mut bi = BlockIterator::new(block);
        bi.seek(Seek::First).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v1", bi.value());
    }

    #[tokio::test]
    async fn test_seek_random() {
        let block = build_block_for_test();
        let mut bi = BlockIterator::new(block);
        bi.seek(Seek::Random(&full_key(b"k3", 3)[..]))
            .await
            .unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v3", bi.value());
    }

    #[tokio::test]
    async fn test_seek_last() {
        let block = build_block_for_test();
        let mut bi = BlockIterator::new(block);
        bi.seek(Seek::Last).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v4", bi.value());
    }

    #[tokio::test]
    async fn test_forward_iterate() {
        let block = build_block_for_test();
        let mut bi = BlockIterator::new(block);

        bi.seek(Seek::First).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v1", bi.value());

        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k2", 2)[..], bi.key());
        assert_eq!(b"v2", bi.value());

        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v3", bi.value());

        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v4", bi.value());

        bi.next().await.unwrap();
        assert!(!bi.is_valid());
    }

    #[tokio::test]
    async fn test_backward_iterate() {
        let block = build_block_for_test();
        let mut bi = BlockIterator::new(block);

        bi.seek(Seek::Last).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v4", bi.value());

        bi.prev().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v3", bi.value());

        bi.prev().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k2", 2)[..], bi.key());
        assert_eq!(b"v2", bi.value());

        bi.prev().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v1", bi.value());

        bi.prev().await.unwrap();
        assert!(!bi.is_valid());
    }
}
