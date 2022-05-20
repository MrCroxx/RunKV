#[derive(thiserror::Error, Debug)]
pub enum RaftLogStoreError {
    #[error("group {0} not exists")]
    GroupNotExists(u64),
    #[error("group {0} already exists")]
    GroupAlreadyExists(u64),
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("decode error: {0}")]
    DecodeError(String),
    #[error("checksum mismatch: [expected: {expected}] [get: {get}]")]
    ChecksumMismatch { expected: u32, get: u32 },
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("raft log gap exists: [{start}, {end})")]
    RaftLogGap { start: u64, end: u64 },
    #[error("raft log file gap: [{start}, {end})")]
    RaftLogFileGap { start: u64, end: u64 },
    #[error("raft log file not found: {0}")]
    RaftLogFileNotFound(u64),
    #[error("other: {0}")]
    Other(String),
}

impl RaftLogStoreError {
    pub fn encode_error(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::EncodeError(e.into().to_string())
    }

    pub fn decode_error(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::DecodeError(e.into().to_string())
    }
}
