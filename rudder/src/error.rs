#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("storage error: {0}")]
    StorageError(#[from] runkv_storage::Error),
    #[error("invalid watermark: [current: {0}] [new: {1}]")]
    InvalidWatermark(u64, u64),
}

pub type Result<T> = std::result::Result<T, Error>;
