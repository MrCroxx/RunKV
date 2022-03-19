pub type Result<T> = std::result::Result<T, anyhow::Error>;

pub fn err(e: impl Into<Box<dyn std::error::Error>>) -> anyhow::Error {
    anyhow::anyhow!("error: {}", e.into())
}

pub fn config_err(e: impl Into<Box<dyn std::error::Error>>) -> anyhow::Error {
    anyhow::anyhow!("config error: {}", e.into())
}
