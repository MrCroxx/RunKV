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

    pub fn storage_err(e: runkv_storage::Error) -> Error {
        Error::StorageError(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
