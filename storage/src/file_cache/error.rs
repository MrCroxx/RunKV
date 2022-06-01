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
    #[error("invalid version")]
    InvalidVersion(u32),
}

pub type Result<T> = core::result::Result<T, Error>;
