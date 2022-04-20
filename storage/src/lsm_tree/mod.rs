pub mod components;
pub mod iterator;
pub mod manifest;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
pub const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64 KiB
pub const DEFAULT_RESTART_INTERVAL: usize = 16;
pub const TEST_DEFAULT_RESTART_INTERVAL: usize = 2;
pub const DEFAULT_ENTRY_SIZE: usize = 1024; // 1 KiB
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;
pub const DEFAULT_SSTABLE_META_SIZE: usize = 4 * 1024; // 4 KiB
pub const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
