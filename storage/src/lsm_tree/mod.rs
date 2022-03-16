pub mod components;
pub mod iterator;
pub mod kv;
pub mod manifest;
pub mod utils;

const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64 KiB
const DEFAULT_RESTART_INTERVAL: usize = 16;
const TEST_DEFAULT_RESTART_INTERVAL: usize = 2;
const DEFAULT_ENTRY_SIZE: usize = 1024; // 1 KiB
const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;
const DEFAULT_SSTABLE_META_SIZE: usize = 4 * 1024; // 4 KiB
#[cfg(test)]
const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
