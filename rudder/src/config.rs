use runkv_storage::manifest::LevelOptions;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
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
    pub meta_cache_capacity: String,
}

#[derive(Deserialize, Debug)]
pub struct LsmTreeConfig {
    pub trigger_l0_compaction_ssts: usize,
    pub trigger_l0_compaction_interval: String,
    pub trigger_compaction_interval: String,
    pub sstable_capacity: String,
    pub block_capacity: String,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub compaction_pin_ttl: String,
    pub levels_options: Vec<LevelOptions>,
}
