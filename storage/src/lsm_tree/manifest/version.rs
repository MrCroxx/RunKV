use std::collections::{BTreeSet, LinkedList};

use runkv_proto::manifest::{SsTableOp, VersionDiff};

use crate::lsm_tree::utils::CompressionAlgorighm;
use crate::{ManifestError, Result};

#[derive(Clone, Copy, Debug)]
pub enum LevelCompactionStrategy {
    Overlap,
    NonOverlap,
}

#[derive(Clone, Debug)]
pub struct LevelOptions {
    pub compaction_strategy: LevelCompactionStrategy,
    pub compression_algorighm: CompressionAlgorighm,
}

#[derive(Debug)]
pub struct Version {
    /// Level compaction strategies for each level.
    ///
    /// Usually, L0 uses `Overlap`, the others use `NonOverlap`.
    level_options: Vec<LevelOptions>,
    /// Sst ids of each level of the lastest version.
    levels: Vec<BTreeSet<u64>>,
    /// List of history version diffs. Used for syncing with other nodes.
    diffs: LinkedList<VersionDiff>,
}

impl Version {
    pub fn new(level_options: Vec<LevelOptions>) -> Self {
        let levels = vec![BTreeSet::default(); level_options.len()];
        Self {
            level_options,
            levels,
            diffs: LinkedList::default(),
        }
    }

    pub fn update(&mut self, diff: VersionDiff) -> Result<()> {
        let current_diff_id = self.diffs.back().map(|diff| diff.id).unwrap_or_else(|| 0);
        if current_diff_id + 1 != diff.id {
            return Err(ManifestError::VersionDiffIdNotMatch(current_diff_id, diff.id).into());
        }

        for (level_idx, level) in diff.levels.iter().enumerate() {
            for sst_diff in &level.sstable_diffs {
                let sst_id = sst_diff.id;
                match sst_diff.op() {
                    SsTableOp::Insert => {
                        if !self.levels[level_idx].insert(sst_id) {
                            return Err(ManifestError::InvalidVersionDiff(format!(
                                "duplicated sst: L{}-{}",
                                level_idx, sst_id
                            ))
                            .into());
                        }
                    }
                    SsTableOp::Delete => {
                        if !self.levels[level_idx].remove(&sst_id) {
                            return Err(ManifestError::InvalidVersionDiff(format!(
                                "sst not exists: L{}-{}",
                                level_idx, sst_id
                            ))
                            .into());
                        }
                    }
                }
            }
        }
        self.diffs.push_back(diff);
        Ok(())
    }

    pub fn level_compression_algorithm(&self, level_idx: u64) -> Result<CompressionAlgorighm> {
        let options =
            self.level_options
                .get(level_idx as usize)
                .ok_or(ManifestError::LevelNotExists(
                    level_idx,
                    self.level_options.len() as u64,
                ))?;
        Ok(options.compression_algorighm.clone())
    }

    pub fn level_compection_strategy(&self, level_idx: u64) -> Result<LevelCompactionStrategy> {
        let options =
            self.level_options
                .get(level_idx as usize)
                .ok_or(ManifestError::LevelNotExists(
                    level_idx,
                    self.level_options.len() as u64,
                ))?;
        Ok(options.compaction_strategy)
    }
}
