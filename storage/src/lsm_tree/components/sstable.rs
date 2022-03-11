use bytes::{BufMut, Bytes, BytesMut};

use super::{
    DEFAULT_BLOCK_SIZE, DEFAULT_BLOOM_FALSE_POSITIVE, DEFAULT_ENTRY_SIZE, DEFAULT_SSTABLE_SIZE,
};
use crate::lsm_tree::utils::CompressionAlgorighm;
use crate::{full_key, BlockBuilder, BlockBuilderOptions, Bloom, Result};

/// [`BlockMeta`] contains block metadata, served as a part of [`Sstable`] meta.
#[derive(Debug)]
pub struct BlockMeta {
    pub offset: usize,
    pub len: usize,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

/// [`Sstable`] serves as a handle to retrieve actuall sstable data from the object store.
#[derive(Debug)]
pub struct Sstable {
    pub id: u64,
    pub meta: SstableMeta,
}

/// [`SstableMeta`] contains sstable metadata.
#[derive(Debug)]
pub struct SstableMeta {
    pub block_metas: Vec<BlockMeta>,
    pub bloom_filter: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// False prsitive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorighm,
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorighm::None,
        }
    }
}

pub struct SstableBuilder {
    /// Options.
    options: SstableBuilderOptions,
    /// Write buffer.
    buf: BytesMut,
    /// Current block builder.
    block_builder: Option<BlockBuilder>,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,
    /// Hashes of user keys.
    user_key_hashes: Vec<u32>,
    /// Last added full key.
    last_full_key: Bytes,
}

impl SstableBuilder {
    pub fn new(options: SstableBuilderOptions) -> Self {
        Self {
            options: options.clone(),
            buf: BytesMut::with_capacity(options.capacity),
            block_builder: None,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            user_key_hashes: Vec::with_capacity(options.capacity / DEFAULT_ENTRY_SIZE + 1),
            last_full_key: Bytes::default(),
        }
    }

    /// Add kv pair to sstable.
    pub fn add(&mut self, user_key: &[u8], timestamp: u64, value: &[u8]) -> Result<()> {
        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_none() {
            self.block_builder = Some(BlockBuilder::new(BlockBuilderOptions {
                capacity: self.options.capacity,
                compression_algorithm: self.options.compression_algorithm.clone(),
            }));
            self.block_metas.push(BlockMeta {
                offset: self.buf.len(),
                len: 0,
                first_key: Bytes::default(),
                last_key: Bytes::default(),
            })
        }

        let block_builder = self.block_builder.as_mut().unwrap();
        let full_key = full_key(user_key, timestamp);

        block_builder.add(&full_key, value);

        self.user_key_hashes.push(farmhash::fingerprint32(user_key));

        if self.last_full_key.is_empty() {
            self.block_metas.last_mut().unwrap().first_key = full_key.clone();
        }
        self.last_full_key = full_key;

        if block_builder.approximate_len() >= self.options.block_capacity {
            self.build_block();
        }
        Ok(())
    }

    /// Finish building sst.
    ///
    /// Unlike most LSM-Tree implementations, sstable meta and data are encoded separately.
    /// Both meta and data has its own object (file).
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub fn build(mut self) -> Result<(SstableMeta, Bytes)> {
        self.build_block();
        self.buf.put_u32_le(self.block_metas.len() as u32);

        let meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter: if self.options.bloom_false_positive > 0.0 {
                let bits_per_key = Bloom::bloom_bits_per_key(
                    self.user_key_hashes.len(),
                    self.options.bloom_false_positive,
                );
                Bloom::build_from_key_hashes(&self.user_key_hashes, bits_per_key).to_vec()
            } else {
                vec![]
            },
        };

        Ok((meta, self.buf.freeze()))
    }

    pub fn approximate_len(&self) -> usize {
        self.buf.len() + 4
    }

    fn build_block(&mut self) {
        // Skip empty block.
        if self.block_builder.is_none() {
            return;
        }
        let mut block_meta = self.block_metas.last_mut().unwrap();
        let block = self.block_builder.take().unwrap().build();
        self.buf.put(&block[..]);
        block_meta.last_key = self.last_full_key.clone();
        block_meta.len = self.buf.len() - block_meta.offset;
        self.last_full_key.clear();
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::*;
    use crate::{Block, BlockIterator, Iterator, Seek};

    #[tokio::test]
    async fn test_sstable_enc_dec() {
        let options = SstableBuilderOptions {
            capacity: 1024,
            block_capacity: 32,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorighm::None,
        };
        let mut builder = SstableBuilder::new(options);
        builder.add(b"k01", 1, b"v01").unwrap();
        builder.add(b"k02", 2, b"v02").unwrap();
        builder.add(b"k04", 4, b"v04").unwrap();
        builder.add(b"k05", 5, b"v05").unwrap();
        let (meta, data) = builder.build().unwrap();
        assert_eq!(2, meta.block_metas.len());
        assert_eq!(&full_key(b"k01", 1), &meta.block_metas[0].first_key);
        assert_eq!(&full_key(b"k02", 2), &meta.block_metas[0].last_key);
        assert_eq!(&full_key(b"k04", 4), &meta.block_metas[1].first_key);
        assert_eq!(&full_key(b"k05", 5), &meta.block_metas[1].last_key);

        let begin = meta.block_metas[0].offset;
        let end = meta.block_metas[0].offset + meta.block_metas[0].len;
        let mut bi = BlockIterator::new(Arc::new(Block::decode(data.slice(begin..end)).unwrap()));
        bi.seek(Seek::First).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], bi.key());
        assert_eq!(b"v01", bi.value());
        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], bi.key());
        assert_eq!(b"v02", bi.value());
        bi.next().await.unwrap();
        assert!(!bi.is_valid());

        let begin = meta.block_metas[1].offset;
        let end = meta.block_metas[1].offset + meta.block_metas[1].len;
        let mut bi = BlockIterator::new(Arc::new(Block::decode(data.slice(begin..end)).unwrap()));
        bi.seek(Seek::First).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], bi.key());
        assert_eq!(b"v04", bi.value());
        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], bi.key());
        assert_eq!(b"v05", bi.value());
        bi.next().await.unwrap();
        assert!(!bi.is_valid());
    }

    #[tokio::test]
    async fn test_compressed_sstable_enc_dec() {
        let options = SstableBuilderOptions {
            capacity: 1024,
            block_capacity: 32,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorighm::Lz4,
        };
        let mut builder = SstableBuilder::new(options);
        builder.add(b"k01", 1, b"v01").unwrap();
        builder.add(b"k02", 2, b"v02").unwrap();
        builder.add(b"k04", 4, b"v04").unwrap();
        builder.add(b"k05", 5, b"v05").unwrap();
        let (meta, data) = builder.build().unwrap();
        assert_eq!(2, meta.block_metas.len());
        assert_eq!(&full_key(b"k01", 1), &meta.block_metas[0].first_key);
        assert_eq!(&full_key(b"k02", 2), &meta.block_metas[0].last_key);
        assert_eq!(&full_key(b"k04", 4), &meta.block_metas[1].first_key);
        assert_eq!(&full_key(b"k05", 5), &meta.block_metas[1].last_key);

        let begin = meta.block_metas[0].offset;
        let end = meta.block_metas[0].offset + meta.block_metas[0].len;
        let mut bi = BlockIterator::new(Arc::new(Block::decode(data.slice(begin..end)).unwrap()));
        bi.seek(Seek::First).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], bi.key());
        assert_eq!(b"v01", bi.value());
        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], bi.key());
        assert_eq!(b"v02", bi.value());
        bi.next().await.unwrap();
        assert!(!bi.is_valid());

        let begin = meta.block_metas[1].offset;
        let end = meta.block_metas[1].offset + meta.block_metas[1].len;
        let mut bi = BlockIterator::new(Arc::new(Block::decode(data.slice(begin..end)).unwrap()));
        bi.seek(Seek::First).await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], bi.key());
        assert_eq!(b"v04", bi.value());
        bi.next().await.unwrap();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], bi.key());
        assert_eq!(b"v05", bi.value());
        bi.next().await.unwrap();
        assert!(!bi.is_valid());
    }
}
