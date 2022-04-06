#[derive(thiserror::Error, Debug)]
pub enum RaftLogStoreError {
    #[error("group {0} not exists")]
    GroupNotExists(u64),
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("other: {0}")]
    Other(String),
}

impl RaftLogStoreError {
    pub fn encode_error(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::EncodeError(e.into().to_string())
    }
}
