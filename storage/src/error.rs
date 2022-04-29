use crate::manifest::ManifestError;
use crate::object_store::ObjectStoreError;
use crate::raft_log_store::error::RaftLogStoreError;
use crate::raft_log_store_v2::error::RaftLogStoreError as RaftLogStoreErrorV2;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("decode error: {0}")]
    DecodeError(String),
    #[error("object store error: {0}")]
    ObjectStoreError(#[from] ObjectStoreError),
    #[error("manifest error: {0}")]
    ManifestError(#[from] ManifestError),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("raft log store error: {0}")]
    RaftLogStoreError(#[from] RaftLogStoreError),
    #[error("raft log store error: {0}")]
    RaftLogStoreErrorV2(#[from] RaftLogStoreErrorV2),
    #[error("other: {0}")]
    Other(String),
}

impl Error {
    pub fn encode_error(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::EncodeError(e.into().to_string())
    }

    pub fn decode_error(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::DecodeError(e.into().to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
