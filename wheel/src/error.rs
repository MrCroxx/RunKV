use runkv_proto::meta::KeyRange;
use tonic::Status;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("config error: {0}")]
    ConfigError(String),
    #[error("storage error: {0}")]
    StorageError(#[from] runkv_storage::Error),
    #[error("transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
    #[error("rpc status error: {0}")]
    RpcStatus(#[from] Status),
    #[error("serde error: {0}")]
    SerdeError(String),
    #[error("raft error: {0}")]
    RaftError(#[from] RaftError),
    #[error("meta error: {0}")]
    MetaError(#[from] MetaError),
    #[error("other: {0}")]
    Other(String),
}

impl Error {
    pub fn err(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::Other(e.into().to_string())
    }

    pub fn config_err(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::ConfigError(e.into().to_string())
    }

    pub fn storage_err(e: runkv_storage::Error) -> Error {
        Self::StorageError(e)
    }

    pub fn serde_err(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::SerdeError(e.into().to_string())
    }

    pub fn status(s: Status) -> Self {
        Self::RpcStatus(s)
    }

    pub fn raft_err(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        RaftError::err(e).into()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum RaftError {
    #[error("raft node not exists: [raft node: {raft_node}] [node: {node}]")]
    RaftNodeNotExists { raft_node: u64, node: u64 },
    #[error("raft node already exists: [group: {group}] [raft node: {raft_node}] [node: {node}]")]
    RaftNodeAlreadyExists {
        group: u64,
        raft_node: u64,
        node: u64,
    },
    #[error("other: {0}")]
    Other(String),
}

impl RaftError {
    pub fn err(e: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self::Other(e.into().to_string())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MetaError {
    #[error("key range overlaps: {r1:?} {r2:?}")]
    KeyRangeOverlaps { r1: KeyRange, r2: KeyRange },
}
