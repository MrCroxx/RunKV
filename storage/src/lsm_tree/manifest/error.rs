#[derive(thiserror::Error, Debug)]
pub enum ManifestError {
    #[error("version diff id does not match: [current: {0}] [new: {1}]")]
    VersionDiffIdNotMatch(u64, u64),
    #[error("invalid version diff: {0}")]
    InvalidVersionDiff(String),
    #[error("level not exists: [idx: {0}] [total: {1}]")]
    LevelNotExists(u64, u64),
}
