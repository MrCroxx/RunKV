// Ported from [AgateDB](https://github.com/tikv/agatedb) with [license](https://github.com/tikv/agatedb/blob/master/LICENSE).

mod arena;
mod key;
mod list;

pub const SKIPLIST_NODE_TOWER_MAX_HEIGHT: usize = 20;

pub use key::{FixedLengthSuffixComparator, KeyComparator};
pub use list::{IterRef, Skiplist};
