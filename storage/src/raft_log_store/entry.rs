use std::io::{BufRead, Read, Write};

use bytes::{Buf, BufMut};
use lz4::Decoder;
use runkv_common::coding::CompressionAlgorithm;

use super::DEFAULT_LOG_BATCH_SIZE;
use crate::error::Result;
use crate::raft_log_store::error::RaftLogStoreError;
use crate::utils::{
    crc32check, crc32sum, get_length_prefixed_slice, put_length_prefixed_slice, var_u32_len,
    BufExt, BufMutExt,
};

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Entry {
    RaftLogBatch(RaftLogBatch),
    Truncate(Truncate),
    Compact(Compact),
    Kv(Kv),
}

impl From<RaftLogBatch> for Entry {
    fn from(f: RaftLogBatch) -> Self {
        Self::RaftLogBatch(f)
    }
}

impl From<Compact> for Entry {
    fn from(f: Compact) -> Self {
        Self::Compact(f)
    }
}

impl From<Kv> for Entry {
    fn from(f: Kv) -> Self {
        Self::Kv(f)
    }
}

impl Entry {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::RaftLogBatch(batch) => {
                buf.put_u8(0);
                batch.encode(buf);
            }
            Self::Truncate(truncate) => {
                buf.put_u8(1);
                truncate.encode(buf);
            }
            Self::Compact(compact) => {
                buf.put_u8(2);
                compact.encode(buf);
            }
            Self::Kv(kv) => {
                buf.put_u8(3);
                kv.encode(buf);
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        match buf.get_u8() {
            0 => Self::RaftLogBatch(RaftLogBatch::decode(buf)),
            1 => Self::Truncate(Truncate::decode(buf)),
            2 => Self::Compact(Compact::decode(buf)),
            3 => Self::Kv(Kv::decode(buf)),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftLogBatch {
    group: u64,
    term: u64,
    first_index: u64,
    offsets: Vec<usize>,
    ctxs: Vec<Vec<u8>>,
    data_len: usize,
    /// Note: Only used for encoding.
    data: Vec<u8>,
}

impl PartialEq for RaftLogBatch {
    fn eq(&self, other: &Self) -> bool {
        self.group == other.group
            && self.term == other.term
            && self.first_index == other.first_index
            && self.offsets == other.offsets
    }
}

impl Eq for RaftLogBatch {}

impl Default for RaftLogBatch {
    fn default() -> Self {
        Self {
            group: 0,
            term: 0,
            first_index: 0,
            offsets: vec![],
            ctxs: vec![],
            data_len: 0,
            data: Vec::with_capacity(DEFAULT_LOG_BATCH_SIZE),
        }
    }
}

impl RaftLogBatch {
    pub fn group(&self) -> u64 {
        self.group
    }

    pub fn first_index(&self) -> u64 {
        self.first_index
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn data_segment_location(&self) -> (usize, usize) {
        let offset = 8 // group
            + 8 // term
            + 8 // first index
            + 8 // N+1
            + self
                .offsets
                .iter()
                .map(|offset| var_u32_len(*offset as u32))
                .sum::<usize>()
            + self.ctxs.iter().map(|ctx| var_u32_len(ctx.len() as u32) + ctx.len()).sum::<usize>()
            + 8; // data segment len
        let len = self.data_len;
        (offset, len)
    }

    pub fn location(&self, index: usize) -> (usize, usize) {
        debug_assert!(index < self.len());
        let offset = self.offsets[index];
        let len = self.offsets[index + 1] - offset;
        (offset, len)
    }

    pub fn ctx(&self, index: usize) -> &[u8] {
        debug_assert!(index < self.len());
        &self.ctxs[index]
    }

    /// Convert raw data to encoded data.
    ///
    /// Format:
    ///
    /// ```plain
    /// | data block (compressed) | compression algorithm (1B) | crc32sum (4B) |
    /// ```
    fn encode_data(&mut self) {
        let mut buf = {
            let mut encoder = lz4::EncoderBuilder::new()
                .level(4)
                .build(Vec::with_capacity(self.data.len()).writer())
                .map_err(RaftLogStoreError::encode_error)
                .unwrap();
            encoder
                .write(&self.data[..])
                .map_err(RaftLogStoreError::encode_error)
                .unwrap();
            let (writer, result) = encoder.finish();
            result.map_err(RaftLogStoreError::encode_error).unwrap();
            writer.into_inner()
        };
        CompressionAlgorithm::Lz4.encode(&mut buf);
        let checksum = crc32sum(&buf);
        buf.put_u32_le(checksum);
        self.data = buf;
        self.data_len = self.data.len()
    }

    /// Format:
    ///
    /// ```plain
    /// | group (8B) | term (8B) | first index (8B) | N+1 (8B) | offset 0 (4B) | ... | offset (N-1) | offset N (phantom) |
    /// | ctx 0 | .. | ctx (N-1) | # ctx is a length prefixed buffer
    /// | data segment len (8B) | data block (compressed) | compression algorithm (1B) | crc32sum (4B) |
    ///                         | <---------- data segment ------------------------------------------->|
    /// ```
    fn encode(&self, mut buf: &mut Vec<u8>) {
        debug_assert!(!self.offsets.is_empty());

        // Encode meta.
        buf.put_u64_le(self.group);
        buf.put_u64_le(self.term);
        buf.put_u64_le(self.first_index);
        buf.put_u64_le(self.offsets.len() as u64);
        for offset in self.offsets.iter() {
            buf.put_var_u32(*offset as u32);
        }
        for ctx in self.ctxs.iter() {
            buf.put_length_prefixed_slice(ctx);
        }
        buf.put_u64_le(self.data_len as u64);
        buf.put_slice(&self.data)
    }

    /// Decode meta only. [`RaftLogBatch.data`] will be left empty.
    pub fn decode(mut buf: &mut &[u8]) -> Self {
        let group = buf.get_u64_le();
        let term = buf.get_u64_le();
        let first_index = buf.get_u64_le();
        let offsets_len = buf.get_u64_le() as usize;
        let mut offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            let offset = buf.get_var_u32() as usize;
            offsets.push(offset);
        }
        let mut ctxs = Vec::with_capacity(offsets_len - 1);
        for _ in 0..offsets_len - 1 {
            let ctx = buf.get_length_prefixed_slice();
            ctxs.push(ctx);
        }
        let data_segment_len = buf.get_u64_le() as usize;
        buf.consume(data_segment_len);
        Self {
            group,
            term,
            first_index,
            offsets,
            ctxs,
            data_len: data_segment_len,
            data: vec![],
        }
    }

    pub fn extract_data_segment(buf: &[u8]) -> Result<Vec<u8>> {
        let checksum = (&buf[buf.len() - 4..]).get_u32_le();
        let buf = &buf[..buf.len() - 4];
        if !crc32check(buf, checksum) {
            return Err(RaftLogStoreError::ChecksumMismatch {
                expected: checksum,
                get: crc32sum(buf),
            }
            .into());
        }
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 1..])
            .map_err(RaftLogStoreError::decode_error)?;
        let buf = &buf[..buf.len() - 1];
        let buf = match compression {
            CompressionAlgorithm::None => buf.to_vec(),
            CompressionAlgorithm::Lz4 => {
                let mut decoder = Decoder::new(buf.reader())
                    .map_err(RaftLogStoreError::decode_error)
                    .unwrap();
                let mut decoded = Vec::with_capacity(buf.len());
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(RaftLogStoreError::decode_error)
                    .unwrap();
                decoded
            }
        };
        Ok(buf)
    }
}

#[derive(Default)]
pub struct RaftLogBatchBuilder {
    pub current: RaftLogBatch,
    pub batches: Vec<RaftLogBatch>,
}

impl RaftLogBatchBuilder {
    pub fn add(&mut self, group: u64, term: u64, index: u64, ctx: &[u8], data: &[u8]) {
        debug_assert_ne!(group, 0);
        debug_assert_ne!(term, 0);
        debug_assert_ne!(index, 0);

        self.may_rotate(group, term, index);

        if self.current.offsets.is_empty() {
            self.current.group = group;
            self.current.term = term;
            self.current.first_index = index;
        }
        self.current.offsets.push(self.current.data.len());
        self.current.ctxs.push(ctx.to_vec());
        self.current.data.put_slice(data);
    }

    pub fn build(mut self) -> Vec<RaftLogBatch> {
        self.may_rotate(0, 0, 0);
        for batch in self.batches.iter_mut() {
            batch.encode_data()
        }
        self.batches
    }

    fn may_rotate(&mut self, group: u64, term: u64, index: u64) {
        if self.current.offsets.is_empty() {
            return;
        }
        if self.current.group != group
            || self.current.term != term
            || self.current.first_index + self.current.offsets.len() as u64 != index
        {
            // Phantom offset.
            self.current.offsets.push(self.current.data.len());
            let mut current = RaftLogBatch::default();
            std::mem::swap(&mut self.current, &mut current);
            self.batches.push(current);
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Truncate {
    pub group: u64,
    pub index: u64,
}

impl Truncate {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(self.group);
        buf.put_u64_le(self.index);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let group = buf.get_u64_le();
        let index = buf.get_u64_le();
        Self { group, index }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Compact {
    pub group: u64,
    pub index: u64,
}

impl Compact {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(self.group);
        buf.put_u64_le(self.index);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let group = buf.get_u64_le();
        let index = buf.get_u64_le();
        Self { group, index }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Kv {
    Put {
        group: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        group: u64,
        key: Vec<u8>,
    },
}

impl Kv {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Kv::Put { group, key, value } => {
                buf.put_u64_le(*group);
                put_length_prefixed_slice(buf, key);
                buf.put_u8(1);
                put_length_prefixed_slice(buf, value);
            }
            Kv::Delete { group, key } => {
                buf.put_u64_le(*group);
                put_length_prefixed_slice(buf, key);
                buf.put_u8(0);
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let group = buf.get_u64_le();
        let key = get_length_prefixed_slice(buf);
        match buf.get_u8() {
            1 => {
                let value = get_length_prefixed_slice(buf);
                Self::Put { group, key, value }
            }
            0 => Self::Delete { group, key },
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_log_enc_dec() {
        let dataset: Vec<(u64, u64, u64, &'static [u8], &'static [u8])> = vec![
            (1, 1, 1, b"aaa", b"data-1-1-1"),
            (1, 1, 2, b"", b"data-1-1-2"),
            (1, 2, 1, b"ccc", b"data-1-2-1"),
            (1, 2, 2, b"ddd", b"data-1-2-2"),
            (2, 1, 1, b"", b"data-2-1-1"),
            (2, 1, 2, b"fff", b"data-2-1-2"),
            (2, 2, 1, b"", b"data-2-2-1"),
            (2, 2, 2, b"", b"data-2-2-2"),
        ];

        let mut builder = RaftLogBatchBuilder::default();
        for (group, term, index, ctx, data) in dataset {
            builder.add(group, term, index, ctx, data);
        }
        let batches = builder.build();
        assert_eq!(batches.len(), 4);

        let logs = [
            Entry::RaftLogBatch(batches[0].clone()),
            Entry::RaftLogBatch(batches[1].clone()),
            Entry::RaftLogBatch(batches[2].clone()),
            Entry::RaftLogBatch(batches[3].clone()),
            Entry::Kv(Kv::Put {
                group: 1,
                key: b"some-key".to_vec(),
                value: b"some-value".to_vec(),
            }),
            Entry::Kv(Kv::Delete {
                group: 1,
                key: b"some-key".to_vec(),
            }),
        ];

        let mut buf = vec![];
        for log in logs.iter() {
            log.encode(&mut buf);
        }
        let mut decoded_logs = vec![];
        let mut buf = &buf[..];
        for _ in 0..logs.len() {
            decoded_logs.push(Entry::decode(&mut buf));
        }
        assert_eq!(decoded_logs, logs);
    }
}
