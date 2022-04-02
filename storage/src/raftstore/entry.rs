use bytes::{Buf, BufMut};

use crate::utils::{get_length_prefixed_slice, put_length_prefixed_slice};

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Entry {
    RaftLog(RaftLog),
    Kv(Kv),
}

impl Entry {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Entry::RaftLog(raft_log) => {
                buf.put_u8(0);
                raft_log.encode(buf);
            }
            Entry::Kv(kv) => {
                buf.put_u8(1);
                kv.encode(buf);
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        match buf.get_u8() {
            0 => Self::RaftLog(RaftLog::decode(buf)),
            1 => Self::Kv(Kv::decode(buf)),
            _ => unreachable!(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum RaftLog {
    Entry {
        group: u64,
        term: u64,
        index: u64,
        data: Vec<u8>,
    },
    Compact {
        group: u64,
        index: u64,
    },
}

impl RaftLog {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            RaftLog::Entry {
                group,
                term,
                index,
                data,
            } => {
                buf.put_u8(0);
                buf.put_u64_le(*group);
                buf.put_u64_le(*term);
                buf.put_u64_le(*index);
                put_length_prefixed_slice(buf, data);
            }
            RaftLog::Compact { group, index } => {
                buf.put_u8(1);
                buf.put_u64_le(*group);
                buf.put_u64_le(*index);
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        match buf.get_u8() {
            0 => {
                let group = buf.get_u64_le();
                let term = buf.get_u64_le();
                let index = buf.get_u64_le();
                let data = get_length_prefixed_slice(buf);
                Self::Entry {
                    group,
                    term,
                    index,
                    data,
                }
            }
            1 => {
                let group = buf.get_u64_le();
                let index = buf.get_u64_le();
                Self::Compact { group, index }
            }
            _ => unreachable!(),
        }
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
        let logs = [
            Entry::RaftLog(RaftLog::Entry {
                group: 1,
                term: 2,
                index: 3,
                data: b"some-data".to_vec(),
            }),
            Entry::RaftLog(RaftLog::Compact { group: 4, index: 5 }),
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
