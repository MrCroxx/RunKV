#[derive(thiserror::Error, Debug)]
pub enum RaftLogStoreError {
    #[error("group {0} not exists")]
    GroupNotExists(u64),
    #[error("group {0} already exists")]
    GroupAlreadyExists(u64),
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("raft log gap exists: [{start}, {end})")]
    RaftLogGap { start: u64, end: u64 },
    #[error("other: {0}")]
    Other(String),
}

impl RaftLogStoreError {
    pub fn encode_error(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::EncodeError(e.into().to_string())
    }
}
