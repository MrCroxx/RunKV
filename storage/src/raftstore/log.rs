use bytes::{Buf, BufMut};

use crate::utils::{get_length_prefixed_slice, put_length_prefixed_slice};

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Log {
    RaftLog(RaftLog),
    Kv(Kv),
}

impl Log {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Log::RaftLog(raft_log) => {
                buf.put_u8(0);
                raft_log.encode(buf);
            }
            Log::Kv(kv) => {
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

    use std::io::SeekFrom;
    use std::time::SystemTime;

    use tempfile::tempdir;
    use test_log::test;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn test_log_enc_dec() {
        let logs = [
            Log::RaftLog(RaftLog::Entry {
                group: 1,
                term: 2,
                index: 3,
                data: b"some-data".to_vec(),
            }),
            Log::RaftLog(RaftLog::Compact { group: 4, index: 5 }),
            Log::Kv(Kv::Put {
                key: b"some-key".to_vec(),
                value: b"some-value".to_vec(),
            }),
            Log::Kv(Kv::Delete {
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
            decoded_logs.push(Log::decode(&mut buf));
        }
        assert_eq!(decoded_logs, logs);
    }

    // TODO: REMOVE ME!!
    #[test(tokio::test)]
    async fn test() {
        let mut options = OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        #[cfg(target_os = "linux")]
        {
            const O_DIRECT: i32 = 0x4000;
            options.custom_flags(O_DIRECT);
        }
        let dir = tokio::task::spawn_blocking(tempdir).await.unwrap().unwrap();
        let mut f = options.open(dir.path().join("tmp")).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let time = SystemTime::now();
        f.write_all(&vec![b'x'; 4096]).await.unwrap();
        f.sync_data().await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![b'x'; 4096]);
        println!("elapsed: {:?}", time.elapsed());
    }
}
