use std::collections::LinkedList;
use std::ops::{Range, RangeInclusive};

use bytes::Bytes;
use runkv_proto::manifest::{SsTableOp, VersionDiff};

use crate::lsm_tree::utils::CompressionAlgorighm;
use crate::{ManifestError, Result, SstableStoreRef};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LevelCompactionStrategy {
    Overlap,
    NonOverlap,
}

#[derive(Clone, Debug)]
pub struct LevelOptions {
    pub compaction_strategy: LevelCompactionStrategy,
    pub compression_algorighm: CompressionAlgorighm,
}

pub struct VersionManagerOptions {
    /// Level compaction and compression strategies for each level.
    ///
    /// Usually, L0 uses `Overlap`, the others use `NonOverlap`.
    pub level_options: Vec<LevelOptions>,
    /// `sstable_store` is used to fetch sstable meta.
    pub sstable_store: SstableStoreRef,
}

pub struct VersionManager {
    /// Level compaction and compression strategies for each level.
    ///
    /// Usually, L0 uses `Overlap`, the others use `NonOverlap`.
    level_options: Vec<LevelOptions>,
    /// Sst ids of each level of the lastest version.
    ///
    /// If the compaction strategy is `NonOverlap`, the sstable ids of the level are guaranteed
    /// sorted in ASC order.
    levels: Vec<Vec<u64>>,
    /// List of history version diffs. Used for syncing with other nodes.
    diffs: LinkedList<VersionDiff>,
    /// `sstable_store` is used to fetch sstable meta.
    sstable_store: SstableStoreRef,
}

impl VersionManager {
    pub fn new(options: VersionManagerOptions) -> Self {
        let levels = vec![vec![]; options.level_options.len()];
        Self {
            level_options: options.level_options,
            levels,
            diffs: LinkedList::default(),
            sstable_store: options.sstable_store,
        }
    }

    pub async fn update(&mut self, diff: VersionDiff) -> Result<()> {
        let current_diff_id = self.diffs.back().map(|diff| diff.id).unwrap_or_else(|| 0);
        if current_diff_id + 1 != diff.id {
            return Err(ManifestError::VersionDiffIdNotMatch(current_diff_id, diff.id).into());
        }

        for sstable_diff in &diff.sstable_diffs {
            let level = sstable_diff.level as usize;
            let compaction_strategy = self
                .level_options
                .get(level)
                .ok_or_else(|| {
                    ManifestError::InvalidVersionDiff(format!("invalid level idx: {}", level))
                })?
                .compaction_strategy;
            match sstable_diff.op() {
                SsTableOp::Insert => {
                    // TODO: Should check duplicated sst id globally.

                    // TODO: Preform async binary search.
                    // Find a position to insert new sst id into.
                    let new_sst_meta = self.sstable_store.meta(sstable_diff.id).await?;
                    let mut idx = 0;
                    while idx < self.levels[level].len() {
                        let meta = self.sstable_store.meta(self.levels[level][idx]).await?;
                        if new_sst_meta.block_metas[0].first_key <= meta.block_metas[0].first_key {
                            break;
                        }
                        idx += 1;
                    }
                    self.levels[level].insert(idx, sstable_diff.id);
                    if compaction_strategy == LevelCompactionStrategy::NonOverlap {
                        // Check overlap.
                        if idx > 0 {
                            let prev_meta =
                                self.sstable_store.meta(self.levels[level][idx - 1]).await?;
                            if new_sst_meta.block_metas[0].first_key
                                <= prev_meta.block_metas.last().as_ref().unwrap().last_key
                            {
                                return Err(ManifestError::InvalidVersionDiff(format!(
                                        "sst overlaps in non-overlap level: [sst: {}, first_key:{:?}, last_key: {:?}] [sst: {}, first_key:{:?}, last_key: {:?}]",
                                        self.levels[level][idx - 1],
                                        prev_meta.block_metas.first().as_ref().unwrap().first_key,
                                        prev_meta.block_metas.last().as_ref().unwrap().last_key,
                                        self.levels[level][idx],
                                        new_sst_meta.block_metas.first().as_ref().unwrap().first_key,
                                        new_sst_meta.block_metas.last().as_ref().unwrap().last_key,
                                    ))
                                    .into());
                            }
                        }
                    }
                }
                SsTableOp::Delete => {
                    if let Some(idx) = self.levels[level]
                        .iter()
                        .position(|&sst_id| sst_id == sstable_diff.id)
                    {
                        self.levels[level].remove(idx);
                    } else {
                        return Err(ManifestError::InvalidVersionDiff(format!(
                            "sst L{}-{} not exists",
                            level, sstable_diff.id
                        ))
                        .into());
                    }
                }
            }
        }

        self.diffs.push_back(diff);
        Ok(())
    }

    /// Revoke all version diffs whose id is smaller than given `diff_id`.
    pub fn squash(&mut self, diff_id: u64) -> Result<()> {
        while self
            .diffs
            .front()
            .as_ref()
            .map_or_else(|| false, |diff| diff.id < diff_id)
        {
            self.diffs.pop_front();
        }
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

    pub fn level_compaction_strategy(&self, level_idx: u64) -> Result<LevelCompactionStrategy> {
        let options =
            self.level_options
                .get(level_idx as usize)
                .ok_or(ManifestError::LevelNotExists(
                    level_idx,
                    self.level_options.len() as u64,
                ))?;
        Ok(options.compaction_strategy)
    }

    /// Pick sstable ids of given `levels` that overlaps with given key `range`.
    /// The length of the retrun vector matches the length of the given levels.
    ///
    /// If the compaction strategy is `NonOverlap`, the retrun sstable ids of the level are
    /// guaranteed sorted in ASC order.
    pub async fn pick_overlap_ssts(
        &self,
        levels: Range<usize>,
        range: RangeInclusive<&Bytes>,
    ) -> Result<Vec<Vec<u64>>> {
        let mut result = vec![vec![]; levels.end - levels.start];
        for level in levels {
            let compaction_strategy = self
                .level_options
                .get(level)
                .ok_or_else(|| {
                    ManifestError::InvalidVersionDiff(format!("invalid level idx: {}", level))
                })?
                .compaction_strategy;
            for sst_id in &self.levels[level] {
                let meta = self.sstable_store.meta(*sst_id).await?;
                if meta.is_overlap_with_range(range.clone()) {
                    result[level].push(*sst_id);
                }
                if compaction_strategy == LevelCompactionStrategy::NonOverlap
                    && meta.block_metas.first().as_ref().unwrap().first_key > range.end()
                {
                    break;
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    // TODO: Fill me please.
}
