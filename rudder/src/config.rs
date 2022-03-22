use runkv_common::config::{CacheConfig, LsmTreeConfig, MinioConfig, S3Config};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct RudderConfig {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub meta_path: String,
    pub health_timeout: String,
    pub s3: Option<S3Config>,
    pub minio: Option<MinioConfig>,
    pub cache: CacheConfig,
    pub lsm_tree: LsmTreeConfig,
}
