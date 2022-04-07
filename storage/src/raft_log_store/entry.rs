use std::io::{BufRead, Write};

use bytes::{Buf, BufMut};
use runkv_common::coding::CompressionAlgorithm;

use super::DEFAULT_LOG_BATCH_SIZE;
use crate::raft_log_store::error::RaftLogStoreError;
use crate::utils::{crc32sum, get_length_prefixed_slice, put_length_prefixed_slice};

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Entry {
    RaftLogBatch(RaftLogBatch),
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
                let mut buf_data = Vec::with_capacity(DEFAULT_LOG_BATCH_SIZE);
                batch.encode(buf, &mut buf_data);
                buf.put_slice(&buf_data);
            }
            Self::Compact(compact) => {
                buf.put_u8(1);
                compact.encode(buf);
            }
            Self::Kv(kv) => {
                buf.put_u8(2);
                kv.encode(buf);
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        match buf.get_u8() {
            0 => Self::RaftLogBatch(RaftLogBatch::decode(buf)),
            1 => Self::Compact(Compact::decode(buf)),
            2 => Self::Kv(Kv::decode(buf)),
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
        let offset = 8 + 8 + 8 + 8 + self.offsets.len() * 4 + 8;
        let len = self.offsets[self.offsets.len() - 1];
        (offset, len)
    }

    pub fn location(&self, index: usize) -> (usize, usize) {
        debug_assert!(index < self.len() - 1);
        let offset = self.offsets[index];
        let len = self.offsets[index + 1] - offset;
        (offset, len)
    }

    /// Format:
    ///
    /// ```plain
    /// | group (8B) | term (8B) | first index (8B) | N+1 (8B) | offset 0 (4B) | ... | offset (N-1) | offset N (phantom) |
    /// | data segment len (8B) | data block (compressed) | compression algorithm (1B) | crc32sum (4B) |
    ///                         | <---------- data segment ------------------------------------------->|
    /// ```
    pub fn encode(&self, buf_meta: &mut Vec<u8>, buf_data: &mut Vec<u8>) {
        debug_assert!(!self.offsets.is_empty());

        // Encode meta.
        buf_meta.put_u64_le(self.group);
        buf_meta.put_u64_le(self.term);
        buf_meta.put_u64_le(self.first_index);
        buf_meta.put_u64_le(self.offsets.len() as u64);
        for offset in self.offsets.iter() {
            buf_meta.put_u32_le(*offset as u32);
        }

        // Encode data.
        let buf = {
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
        let checksum = crc32sum(&buf);
        let len = buf.len() + 1 + 4;
        buf_data.put_u64_le(len as u64);
        buf_data.put_slice(&buf);
        CompressionAlgorithm::Lz4.encode(buf_data);
        buf_data.put_u32_le(checksum);
    }

    /// Decode meta only. [`RaftLogBatch.data`] will be left empty.
    pub fn decode(buf: &mut &[u8]) -> Self {
        let group = buf.get_u64_le();
        let term = buf.get_u64_le();
        let first_index = buf.get_u64_le();
        let offsets_len = buf.get_u64_le() as usize;
        let mut offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            let offset = buf.get_u32_le() as usize;
            offsets.push(offset);
        }
        let data_segment_len = buf.get_u64_le() as usize;
        buf.consume(data_segment_len);
        Self {
            group,
            term,
            first_index,
            offsets,
            data: vec![],
        }
    }
}

#[derive(Default)]
pub struct RaftLogBatchBuilder {
    pub current: RaftLogBatch,
    pub batches: Vec<RaftLogBatch>,
}

impl RaftLogBatchBuilder {
    pub fn add(&mut self, group: u64, term: u64, index: u64, data: &[u8]) {
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
        self.current.data.put_slice(data);
    }

    pub fn build(mut self) -> Vec<RaftLogBatch> {
        self.may_rotate(0, 0, 0);
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
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Kv {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Kv::Put { key, value } => {
                put_length_prefixed_slice(buf, key);
                buf.put_u8(1);
                put_length_prefixed_slice(buf, value);
            }
            Kv::Delete { key } => {
                put_length_prefixed_slice(buf, key);
                buf.put_u8(0);
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let key = get_length_prefixed_slice(buf);
        match buf.get_u8() {
            1 => {
                let value = get_length_prefixed_slice(buf);
                Self::Put { key, value }
            }
            0 => Self::Delete { key },
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;

    #[test]
    fn test_log_enc_dec() {
        let dataset = vec![
            (1, 1, 1, b"data-1-1-1"),
            (1, 1, 2, b"data-1-1-2"),
            (1, 2, 1, b"data-1-2-1"),
            (1, 2, 2, b"data-1-2-2"),
            (2, 1, 1, b"data-2-1-1"),
            (2, 1, 2, b"data-2-1-2"),
            (2, 2, 1, b"data-2-2-1"),
            (2, 2, 2, b"data-2-2-2"),
        ];

        let mut builder = RaftLogBatchBuilder::default();
        for (group, term, index, data) in dataset {
            builder.add(group, term, index, data);
        }
        let batches = builder.build();
        assert_eq!(batches.len(), 4);

        let logs = [
            Entry::RaftLogBatch(batches[0].clone()),
            Entry::RaftLogBatch(batches[1].clone()),
            Entry::RaftLogBatch(batches[2].clone()),
            Entry::RaftLogBatch(batches[3].clone()),
            Entry::Kv(Kv::Put {
                key: b"some-key".to_vec(),
                value: b"some-value".to_vec(),
            }),
            Entry::Kv(Kv::Delete {
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
