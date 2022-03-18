#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("config error: {0}")]
    ConfigError(String),
    #[error("other: {0}")]
    Other(String),
}

pub fn err(e: impl Into<Box<dyn std::error::Error>>) -> Error {
    Error::Other(e.into().to_string())
}

pub fn config_err(e: impl Into<Box<dyn std::error::Error>>) -> Error {
    Error::ConfigError(e.into().to_string())
}

pub type Result<T> = std::result::Result<T, Error>;
