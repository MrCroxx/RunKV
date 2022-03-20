use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ExhausterConfig {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub meta_path: String,
    pub heartbeat_interval: String,
    pub rudder: RudderConfig,
    pub s3: Option<S3Config>,
    pub minio: Option<MinioConfig>,
    pub cache: CacheConfig,
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
pub struct CacheConfig {
    pub block_cache_capacity: String,
    pub meta_cache_capacity: String,
}

#[derive(Deserialize, Debug)]
pub struct RudderConfig {
    pub host: String,
    pub port: u16,
}
