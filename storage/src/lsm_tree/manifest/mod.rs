use std::collections::LinkedList;

#[derive(Clone, Copy, Debug)]
pub enum LevelCompactionStrategy {
    Overlap,
    NonOverlap,
}

#[derive(Clone, Copy, Debug)]
pub enum SstableOp {
    Insert,
    Delete,
}

#[derive(Clone, Debug)]
pub struct VersionDiff {
    pub levels: Vec<Vec<(u64, SstableOp)>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Version {
    /// Level compaction strategies for each level.
    ///
    /// Usually, L0 uses `Overlap`, the others use `NonOverlap`.
    level_comapction_strategies: Vec<LevelCompactionStrategy>,
    /// Sst ids of each level of the lastest version.
    levels: Vec<Vec<u64>>,
    /// List of history version diffs. Used for syncing with other nodes.
    diffs: LinkedList<VersionDiff>,
    /// Version id of the first version diff in `diffs`.
    first_diff_id: u64,
}
