use std::ops::{Range, RangeBounds};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::utils::{crc32sum, CompressionAlgorighm};
use super::{key_diff, user_key};
use crate::sstable::utils::crc32check;
use crate::{Error, Result};

const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64KiB
const DEFAULT_RESTART_COUNT: usize = 16;
const DEFAULT_ENTRY_SIZE: usize = 1024; // 1 KiB

pub struct Block {
    /// Uncompressed entries data.
    data: Bytes,
    /// Restart points.
    restart_points: Vec<u32>,
}

impl Block {
    pub fn decode(buf: &Bytes) -> Result<Self> {
        // Verify checksum.
        let crc32sum = (&buf[buf.len() - 4..]).get_u32_le();
        if !crc32check(&buf[..buf.len() - 4], crc32sum) {
            return Err(Error::DecodeError("invalid checksum".to_string()));
        }

        // Decompress.
        let compression = CompressionAlgorighm::decode(&mut &buf[buf.len() - 5..buf.len() - 4])?;
        let buf = match compression {
            CompressionAlgorighm::None => buf.slice(..buf.len() - 5),
            CompressionAlgorighm::LZ4 => {
                use lzzzz::lz4;
                let mut decoded = BytesMut::with_capacity(buf.len() - 5);
                lz4::decompress(&buf[..buf.len() - 5], &mut decoded)
                    .map_err(Error::decode_error)?;
                decoded.freeze()
            }
        };

        // Decode restart points.
        let n_restarts = (&buf[buf.len() - 4..]).get_u32_le();
        let data_len = buf.len() - 4 - n_restarts as usize * 4;
        let mut restart_points = Vec::with_capacity(n_restarts as usize);
        let mut restart_points_buf = &buf[data_len..buf.len() - 4];
        for _ in 0..n_restarts {
            restart_points.push(restart_points_buf.get_u32_le());
        }

        Ok(Block {
            data: buf.slice(..data_len),
            restart_points,
        })
    }

    /// Entries data len.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        assert!(!self.data.is_empty());
        self.data.len()
    }

    /// Get restart point by index.
    pub fn restart_point(&self, index: usize) -> u32 {
        self.restart_points[index]
    }

    /// Decode [`KeyPrefix`] at given index.
    pub fn decode_prefix_at(&self, index: usize) -> KeyPrefix {
        KeyPrefix::decode(&mut &self.data[index..index + KeyPrefix::len()])
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> Bytes {
        self.data.slice(range)
    }
}

/// [`KeyPrefix`] contains info for prefix compression.
#[derive(Debug)]
pub struct KeyPrefix {
    pub overlap: usize,
    pub diff: usize,
    pub value: usize,
}

impl KeyPrefix {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16_le(self.overlap as u16);
        buf.put_u16_le(self.diff as u16);
        buf.put_u32_le(self.value as u32);
    }

    pub fn decode(buf: &mut impl Buf) -> Self {
        let overlap = buf.get_u16_le() as usize;
        let diff = buf.get_u16_le() as usize;
        let value = buf.get_u32_le() as usize;
        Self {
            overlap,
            diff,
            value,
        }
    }

    /// Encoded length.
    pub fn len() -> usize {
        8
    }
}

pub struct BlockBuilderOptions {
    /// Reserved bytes size when creating buffer to avoid frequent allocating.
    reserved: usize,
    /// Compression algorithm.
    compression_algorithm: CompressionAlgorighm,
}

impl Default for BlockBuilderOptions {
    fn default() -> Self {
        Self {
            reserved: DEFAULT_BLOCK_SIZE,
            compression_algorithm: CompressionAlgorighm::None,
        }
    }
}

/// [`BlockWriter`] encode and append block to a buffer.
pub struct BlockBuilder {
    /// Write buffer.
    buf: BytesMut,
    /// Entry interval between restart points.
    restart_count: usize,
    /// Restart points.
    restart_points: Vec<u32>,
    /// Last key.
    last_key: Bytes,
    /// Count of entries in current block.
    entry_count: usize,
    /// Compression algorithm.
    compression_algorithm: CompressionAlgorighm,
}

impl BlockBuilder {
    pub fn new(options: BlockBuilderOptions) -> Self {
        Self {
            buf: BytesMut::with_capacity(options.reserved),
            restart_count: DEFAULT_RESTART_COUNT,
            restart_points: Vec::with_capacity(
                options.reserved / DEFAULT_ENTRY_SIZE / DEFAULT_RESTART_COUNT + 1,
            ),
            last_key: Bytes::default(),
            entry_count: 0,
            compression_algorithm: options.compression_algorithm,
        }
    }

    /// Append a kv pair to the block.
    ///
    /// NOTE: Key must be added in ASCEND order.
    ///
    /// # Format
    ///
    ///     entry (kv pair): | overlap len (2B) | diff len (2B) | value len(4B) | diff key | value |
    ///
    /// # Panics
    ///
    ///     Panic if key is not added in ASCEND order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.entry_count > 0 {
            assert!(self.last_key < key);
        }
        // Update restart point if needed and calculate diff key.
        let diff_key = if self.entry_count % self.restart_count == 0 {
            self.restart_points.push(self.buf.len() as u32);
            self.last_key = key.to_vec().into();
            key
        } else {
            key_diff(&self.last_key, key)
        };

        let prefix = KeyPrefix {
            overlap: key.len() - diff_key.len(),
            diff: diff_key.len(),
            value: value.len(),
        };

        prefix.encode(&mut self.buf);
        self.buf.put_slice(diff_key);
        self.buf.put_slice(value);

        self.last_key = key.to_vec().into();
        self.entry_count += 1;
    }

    /// Finish building sst.
    ///
    /// # Format
    /// ```plain
    ///     compressed: | entries | restart point 0 (4B) | ... | restart point N-1 (4B) | N (4B) |
    ///     uncompressed: | compression method (1B) | crc32sum (4B) |
    /// ```
    ///
    /// # Panics
    ///
    ///     Panic if there is compression error.
    pub fn build(mut self) -> Bytes {
        for restart_point in &self.restart_points {
            self.buf.put_u32_le(*restart_point);
        }
        self.buf.put_u32_le(self.restart_points.len() as u32);
        let mut buf = match self.compression_algorithm {
            CompressionAlgorighm::None => self.buf,
            CompressionAlgorighm::LZ4 => {
                use lzzzz::lz4;
                let mut encoded = BytesMut::with_capacity(self.buf.len());
                lz4::compress(&self.buf, &mut encoded, lz4::ACC_LEVEL_DEFAULT)
                    .map_err(Error::encode_error)
                    .unwrap();
                encoded
            }
        };
        self.compression_algorithm.encode(&mut buf);
        let checksum = crc32sum(&buf);
        buf.put_u32_le(checksum);
        buf.freeze()
    }

    /// Approximate block len (uncompressed).
    pub fn approximate_len(&self) -> usize {
        self.buf.len() + 4 * self.restart_points.len() + 4 + 1 + 4
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::test_utils::full_key;
    use crate::{BlockIterator, Iterator};

    #[tokio::test]
    async fn test_block_enc_dec() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k1", 1), b"v1");
        builder.add(&full_key(b"k2", 2), b"v2");
        builder.add(&full_key(b"k3", 3), b"v3");
        builder.add(&full_key(b"k4", 4), b"v4");
        let buf = builder.build();
        let block = Arc::new(Block::decode(&buf).unwrap());
        let mut bi = BlockIterator::new(block);

        // TODO: replace this with seek first.
        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(full_key(b"k1", 1), bi.key());

        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(full_key(b"k2", 2), bi.key());

        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(full_key(b"k3", 3), bi.key());

        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(full_key(b"k4", 4), bi.key());

        bi.next().await.unwrap();
        assert!(!bi.is_valid());
    }
}
