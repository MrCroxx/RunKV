mod block;
pub use block::*;
mod bloom;
pub use bloom::*;
mod key;
pub use key::*;
mod sstable;
pub use sstable::*;

const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64KiB
const DEFAULT_RESTART_COUNT: usize = 16;
const DEFAULT_ENTRY_SIZE: usize = 1024; // 1 KiB
const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;
