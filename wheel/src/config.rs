use runkv_common::config::{
    CacheConfig, LsmTreeConfig, MinioConfig, Node, PrometheusConfig, S3Config,
};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct WheelConfig {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub log: String,
    pub data_path: String,
    pub meta_path: String,
    pub poll_interval: String,
    pub heartbeat_interval: String,
    pub rudder: Node,
    pub s3: Option<S3Config>,
    pub minio: Option<MinioConfig>,
    pub buffer: BufferConfig,
    pub cache: CacheConfig,
    pub lsm_tree: LsmTreeConfig,
    pub raft_log_store: RaftLogStoreConfig,
    pub tiered_cache: TieredCacheConfig,
    pub prometheus: PrometheusConfig,
}

#[derive(Deserialize, Clone, Debug)]
pub struct BufferConfig {
    pub write_buffer_capacity: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct RaftLogStoreConfig {
    pub log_dir_path: String,
    pub log_file_capacity: String,
    pub block_cache_capacity: String,
    pub persist: String,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", content = "args")]
pub enum TieredCacheConfig {
    None,
    FileCache(FileCacheConfig),
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FileCacheConfig {
    pub dir: String,
    pub capacity: String,
    pub total_buffer_capacity: String,
    pub cache_file_fallocate_unit: String,
    pub cache_meta_fallocate_unit: String,
    pub cache_file_max_write_size: String,
}

#[cfg(test)]
mod tests {
    use super::{FileCacheConfig, TieredCacheConfig};

    #[test]
    fn test_tiered_cache_config_parse() {
        let text = r#"type = "None""#;
        let config: TieredCacheConfig = toml::from_str(text).unwrap();
        assert_eq!(TieredCacheConfig::None, config);

        let text = r#"
type = "FileCache"
[args]
dir = "<dir>"
capacity = "<capacity>"
total_buffer_capacity = "<total_buffer_capacity>"
cache_file_fallocate_unit = "<cache_file_fallocate_unit>"
cache_meta_fallocate_unit = "<cache_meta_fallocate_unit>"
cache_file_max_write_size = "<cache_file_max_write_size>"
"#;
        let config: TieredCacheConfig = toml::from_str(text).unwrap();
        assert_eq!(
            TieredCacheConfig::FileCache(FileCacheConfig {
                dir: "<dir>".to_string(),
                capacity: "<capacity>".to_string(),
                total_buffer_capacity: "<total_buffer_capacity>".to_string(),
                cache_file_fallocate_unit: "<cache_file_fallocate_unit>".to_string(),
                cache_meta_fallocate_unit: "<cache_meta_fallocate_unit>".to_string(),
                cache_file_max_write_size: "<cache_file_max_write_size>".to_string(),
            }),
            config
        );
    }
}
