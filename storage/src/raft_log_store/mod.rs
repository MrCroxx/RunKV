pub mod entry;
pub mod error;
pub mod log;
#[allow(dead_code)]
pub mod mem;
#[allow(dead_code)]
pub mod store;

const DEFAULT_LOG_BATCH_SIZE: usize = 8 << 10;
