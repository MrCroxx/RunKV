use runkv_storage::manifest::LevelOptions;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct WheelConfig {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub meta_path: String,
    pub poll_interval: String,
    pub heartbeat_interval: String,
    pub rudder: RudderConfig,
    pub s3: Option<S3Config>,
    pub minio: Option<MinioConfig>,
    pub buffer: BufferConfig,
    pub cache: CacheConfig,
    pub lsm_tree: LsmTreeConfig,
}

// TODO: Fill me.
#[derive(Deserialize, Debug)]
pub struct S3Config {
    pub bucket: String,
}

#[derive(Deserialize, Debug)]
pub struct MinioConfig {
    pub url: String,
}

#[derive(Deserialize, Debug)]
pub struct BufferConfig {
    pub write_buffer_capacity: String,
}

#[derive(Deserialize, Debug)]
pub struct CacheConfig {
    pub block_cache_capacity: String,
    pub meta_cache_capacity: String,
}

#[derive(Deserialize, Debug)]
pub struct LsmTreeConfig {
    pub sstable_capacity: String,
    pub block_capacity: String,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub levels_options: Vec<LevelOptions>,
}

#[derive(Deserialize, Debug)]
pub struct RudderConfig {
    pub host: String,
    pub port: u16,
}
