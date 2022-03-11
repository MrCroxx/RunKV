use bytes::{BufMut, Bytes, BytesMut};

use crate::lsm_tree::utils::CompressionAlgorighm;
use crate::{full_key, BlockBuilder, BlockBuilderOptions, Bloom, Result, DEFAULT_ENTRY_SIZE};

/// [`BlockMeta`] contains block metadata, served as a part of [`Sstable`] meta
pub struct BlockMeta {
    pub offset: usize,
    pub len: usize,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

/// [`Sstable`] serves as a handle to retrieve actuall sstable data from the object store.
pub struct Sstable {
    pub id: u64,
    pub meta: SstableMeta,
}

/// [`SstableMeta`] contains sstable metadata.
pub struct SstableMeta {
    pub block_metas: Vec<BlockMeta>,
    pub bloom_filter: Vec<u8>,
}

impl SstableMeta {}

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
            self.last_full_key = full_key;
        }

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
    #[tokio::test]
    async fn test_sstable_enc_dec() {
        // todo!()
    }
}
