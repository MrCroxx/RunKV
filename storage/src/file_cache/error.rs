#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("nix error: {0}")]
    NixError(#[from] nix::errno::Errno),
    #[error("join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("magic not match")]
    MagicNotMatch,
    #[error("magic file not found")]
    MagicFileNotFound,
    #[error("invalid version")]
    InvalidVersion(u64),
    #[error("cache file full")]
    Full,
    #[error("unsupported fs: [super block magic: {0}]")]
    UnsupportedFs(u64),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = core::result::Result<T, Error>;
