use std::str::FromStr;

use serde::Deserialize;

use crate::coding::CompressionAlgorithm;

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum LevelCompactionStrategy {
    Overlap,
    NonOverlap,
}

#[derive(Deserialize, Clone, Debug)]
pub struct LevelOptions {
    pub compaction_strategy: LevelCompactionStrategy,
    pub compression_algorithm: CompressionAlgorithm,
}

#[derive(Deserialize, Clone, Default, Debug)]
pub struct LsmTreeConfig {
    pub l1_capacity: String,
    pub level_multiplier: usize,
    pub trigger_l0_compaction_ssts: usize,
    pub trigger_l0_compaction_interval: String,
    pub trigger_lmax_compaction_interval: String,
    pub trigger_compaction_interval: String,
    pub sstable_capacity: String,
    pub block_capacity: String,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub compaction_pin_ttl: String,
    pub levels_options: Vec<LevelOptions>,
}

impl FromStr for LsmTreeConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c = toml::from_str(s)?;
        Ok(c)
    }
}

// TODO: Fill me.
#[derive(Deserialize, Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct MinioConfig {
    pub url: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct CacheConfig {
    pub block_cache_capacity: String,
    pub meta_cache_capacity: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Node {
    pub id: u64,
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PrometheusConfig {
    pub host: String,
    pub port: u16,
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;

    #[test]
    fn lsm_tree_config_serde() {
        let s = r#"
        l1_capacity = "1 MiB"
        level_multiplier = 10
        
        trigger_l0_compaction_ssts = 4
        trigger_l0_compaction_interval = "1 s"
        trigger_lmax_compaction_interval = "10 s"
        trigger_compaction_interval = "5 s"
        
        sstable_capacity = "64 KiB"
        block_capacity = "4 KiB"
        restart_interval = 2
        bloom_false_positive = 0.1
        
        compaction_pin_ttl = "15 s"
        
        [[levels_options]]
        compaction_strategy = "Overlap"
        compression_algorithm = "None"
        
        [[levels_options]]
        compaction_strategy = "NonOverlap"
        compression_algorithm = "None"
        
        [[levels_options]]
        compaction_strategy = "NonOverlap"
        compression_algorithm = "None"
        
        [[levels_options]]
        compaction_strategy = "NonOverlap"
        compression_algorithm = "None"
        
        [[levels_options]]
        compaction_strategy = "NonOverlap"
        compression_algorithm = "Lz4"
        
        [[levels_options]]
        compaction_strategy = "NonOverlap"
        compression_algorithm = "Lz4"
        
        [[levels_options]]
        compaction_strategy = "NonOverlap"
        compression_algorithm = "Lz4""#;
        LsmTreeConfig::from_str(s).unwrap();
    }
}
