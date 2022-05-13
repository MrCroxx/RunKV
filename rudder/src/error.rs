use runkv_proto::common::Endpoint;
use runkv_proto::meta::KeyRange;
use tonic::Status;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("storage error: {0}")]
    StorageError(#[from] runkv_storage::Error),
    #[error("invalid watermark: [current: {0}] [new: {1}]")]
    InvalidWatermark(u64, u64),
    #[error("transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
    #[error("rpc status error: {0}")]
    RpcStatus(#[from] Status),
    #[error("config error: {0}")]
    ConfigError(String),
    #[error("control error: {0}")]
    ControlError(#[from] ControlError),
    #[error("other: {0}")]
    Other(String),
}

impl Error {
    pub fn err(e: impl Into<Box<dyn std::error::Error>>) -> Error {
        Error::Other(e.into().to_string())
    }

    pub fn config_err(e: impl Into<Box<dyn std::error::Error>>) -> Error {
        Error::ConfigError(e.into().to_string())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ControlError {
    #[error("node already exists: [node: {node}] [origin endpoint: {origin:?}] [given endpoint: {given:?}]")]
    NodeAlreadyExists {
        node: u64,
        origin: Endpoint,
        given: Endpoint,
    },
    #[error("node not exists: {0}")]
    NodeNotExists(u64),
    #[error("group already exists: {0}")]
    GroupAlreadyExists(u64),
    #[error("group not exists: {0}")]
    GroupNotExists(u64),
    #[error("raft node already exists: {0}")]
    RaftNodeAlreadyExists(u64),
    #[error("raft node not exists: {0}")]
    RaftNodeNotExists(u64),
    #[error("key range overlaps: [{0:?}] [{1:?}]")]
    KeyRangeOverlaps(KeyRange, KeyRange),
}

pub type Result<T> = std::result::Result<T, Error>;
