use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use super::{Iterator, Seek};
use crate::{Block, Error, KeyPrefix, Result};

/// [`BlockIteror`] is used to read kv pairs in a block.
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

#[async_trait]
impl Iterator for BlockIterator {
    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.offset += self.entry_len;
        if self.is_valid() {
            let prefix = self.block.decode_prefix_at(self.offset);
            self.key.truncate(prefix.overlap);
            self.key.extend_from_slice(&self.block.slice(
                self.offset + KeyPrefix::len()..self.offset + KeyPrefix::len() + prefix.diff,
            ));
            self.entry_len = KeyPrefix::len() + prefix.diff + prefix.value;
        } else {
            self.key.clear();
            self.value.clear();
            self.entry_len = 0;
        }
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
        self.offset <= self.block.len()
    }

    async fn seek<'s>(&mut self, position: Seek<'s>) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
