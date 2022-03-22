use runkv_common::config::{CacheConfig, MinioConfig, RudderServiceConfig, S3Config};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct ExhausterConfig {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub meta_path: String,
    pub heartbeat_interval: String,
    pub rudder: RudderServiceConfig,
    pub s3: Option<S3Config>,
    pub minio: Option<MinioConfig>,
    pub cache: CacheConfig,
}
