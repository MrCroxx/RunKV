use tonic::Status;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("rpc status error: {0}")]
    RpcStatus(#[from] Status),
    #[error("kv error: {0}")]
    KvError(#[from] KvError),
    #[error("config error: {0}")]
    ConfigError(String),
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

    pub fn redirect(&self) -> bool {
        matches!(self, Self::KvError(KvError::Redirect))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum KvError {
    #[error("temporarily no leader for key: {0:?}")]
    TemporarilyNoLeader(Vec<u8>),
    #[error("valid leader changed, need redirect")]
    Redirect,
}

pub type Result<T> = std::result::Result<T, Error>;
