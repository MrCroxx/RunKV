use runkv_common::config::{CacheConfig, MinioConfig, Node, S3Config};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct ExhausterConfig {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub meta_path: String,
    pub heartbeat_interval: String,
    pub rudder: Node,
    pub s3: Option<S3Config>,
    pub minio: Option<MinioConfig>,
    pub cache: CacheConfig,
}
