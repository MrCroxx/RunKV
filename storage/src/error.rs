#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("decode error: {0}")]
    DecodeError(String),
}

impl Error {
    pub fn encode_error(e: impl std::error::Error) -> Self {
        Self::EncodeError(e.to_string())
    }

    pub fn decode_error(e: impl std::error::Error) -> Self {
        Self::DecodeError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
