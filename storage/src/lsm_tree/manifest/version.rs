use std::collections::VecDeque;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use runkv_common::coding::CompressionAlgorithm;
use runkv_common::config::{LevelCompactionStrategy, LevelOptions};
use runkv_proto::manifest::{SstableOp, VersionDiff};
use tokio::sync::RwLock;
use tracing::trace;

use super::ManifestError;
use crate::components::SstableStoreRef;
use crate::utils::user_key;
use crate::Result;

pub struct VersionManagerOptions {
    /// Level compaction and compression strategies for each level.
    ///
    /// Usually, L0 uses `Overlap`, the others use `NonOverlap`.
    pub levels_options: Vec<LevelOptions>,
    /// Initial sst ids of each level.
    ///
    /// If the compaction strategy is `NonOverlap`, the sstable ids of the level must be guaranteed
    /// sorted in ASC order.
    pub levels: Vec<Vec<u64>>,
    /// `sstable_store` is used to fetch sstable meta.
    pub sstable_store: SstableStoreRef,
}

pub struct VersionManagerCore {
    /// Level compaction and compression strategies for each level.
    ///
    /// Usually, L0 uses `Overlap`, the others use `NonOverlap`.
    level_options: Vec<LevelOptions>,
    /// Sst ids of each level of the lastest version.
    ///
    /// If the compaction strategy is `NonOverlap`, the sstable ids of the level are guaranteed
    /// sorted in ASC order.
    levels: Vec<Vec<u64>>,
    /// SSTable data files size of each level.
    levels_data_size: Vec<usize>,
    /// List of history version diffs. Used for syncing with other nodes.
    ///
    /// TODO: Restore diff from `MetaStore`.
    diffs: VecDeque<VersionDiff>,
    /// `sstable_store` is used to fetch sstable meta.
    sstable_store: SstableStoreRef,
    /// Minimum accessable sequence.
    watermark: u64,
}

impl VersionManagerCore {
    fn new(options: VersionManagerOptions) -> Self {
        assert_eq!(options.levels.len(), options.levels_options.len());
        Self {
            level_options: options.levels_options,
            levels_data_size: vec![0; options.levels.len()],
            levels: options.levels,
            diffs: VecDeque::default(),
            sstable_store: options.sstable_store,
            watermark: 0,
        }
    }

    fn levels(&self) -> usize {
        self.levels.len()
    }

    fn level_data_size(&self, level_idx: usize) -> usize {
        self.levels_data_size[level_idx]
    }

    fn watermark(&self) -> u64 {
        self.watermark
    }

    fn advance(&mut self, watermark: u64) -> Result<()> {
        if watermark < self.watermark {
            return Err(ManifestError::InvalidWatermark(self.watermark, watermark).into());
        }
        self.watermark = watermark;
        Ok(())
    }

    fn latest_version_id(&self) -> u64 {
        self.diffs.back().map_or_else(|| 0, |diff| diff.id)
    }

    async fn update(&mut self, mut diff: VersionDiff, sync: bool) -> Result<()> {
        if sync {
            let current_diff_id = self.diffs.back().map(|diff| diff.id).unwrap_or_else(|| 0);
            if !self.diffs.is_empty() && current_diff_id + 1 != diff.id {
                return Err(ManifestError::VersionDiffIdNotMatch(current_diff_id, diff.id).into());
            }
        } else {
            let diff_id = self.diffs.back().map_or_else(|| 1, |diff| diff.id + 1);
            diff.id = diff_id;
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
                SstableOp::Insert => {
                    // TODO: Should check duplicated sst id globally.

                    // TODO: Preform async binary search.
                    // Find a position to insert new sst id into.
                    let sst_to_insert = self.sstable_store.sstable(sstable_diff.id).await?;
                    let mut idx = 0;
                    while idx < self.levels[level].len() {
                        let sst = self.sstable_store.sstable(self.levels[level][idx]).await?;
                        if sst_to_insert.first_key() <= sst.first_key() {
                            break;
                        }
                        idx += 1;
                    }
                    self.levels[level].insert(idx, sstable_diff.id);
                    self.levels_data_size[level] += sstable_diff.data_size as usize;
                    if compaction_strategy == LevelCompactionStrategy::NonOverlap {
                        // Check overlap.
                        if idx > 0 {
                            let prev_sst = self
                                .sstable_store
                                .sstable(self.levels[level][idx - 1])
                                .await?;
                            if sst_to_insert.first_key() <= prev_sst.last_key() {
                                return Err(ManifestError::InvalidVersionDiff(format!(
                                        "sst overlaps in non-overlap level: [sst: {}, first_key:{:?}, last_key: {:?}] [sst: {}, first_key:{:?}, last_key: {:?}]",
                                        self.levels[level][idx - 1],
                                        prev_sst.first_key(),
                                        prev_sst.last_key(),
                                        self.levels[level][idx],
                                        sst_to_insert.first_key(),
                                        sst_to_insert.last_key(),
                                    ))
                                    .into());
                            }
                        }
                    }
                }
                SstableOp::Delete => {
                    if let Some(idx) = self.levels[level]
                        .iter()
                        .position(|&sst_id| sst_id == sstable_diff.id)
                    {
                        self.levels[level].remove(idx);
                        self.levels_data_size[level] -= sstable_diff.data_size as usize;
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
        if !sync {
            trace!("updated levels: {:?}", self.levels);
            trace!("updated levels size: {:#?}", self.levels_data_size);
        }
        Ok(())
    }

    /// Revoke all version diffs whose id is smaller than given `diff_id`.
    fn squash(&mut self, diff_id: u64) {
        while self
            .diffs
            .front()
            .as_ref()
            .map_or_else(|| false, |diff| diff.id < diff_id)
        {
            self.diffs.pop_front();
        }
    }

    fn level_compression_algorithm(&self, level_idx: u64) -> Result<CompressionAlgorithm> {
        let options =
            self.level_options
                .get(level_idx as usize)
                .ok_or(ManifestError::LevelNotExists(
                    level_idx,
                    self.level_options.len() as u64,
                ))?;
        Ok(options.compression_algorithm)
    }

    fn level_compaction_strategy(&self, level_idx: u64) -> Result<LevelCompactionStrategy> {
        let options =
            self.level_options
                .get(level_idx as usize)
                .ok_or(ManifestError::LevelNotExists(
                    level_idx,
                    self.level_options.len() as u64,
                ))?;
        Ok(options.compaction_strategy)
    }

    /// Get at most `max_len` version diffs from given `start_id`.
    fn version_diffs_from(&self, start_id: u64, max_len: usize) -> Result<Vec<VersionDiff>> {
        if self.diffs.is_empty() || start_id < self.diffs.front().as_ref().unwrap().id {
            return Err(ManifestError::VersionDiffExpired(start_id).into());
        }
        let mut diffs = Vec::with_capacity(std::cmp::min(max_len, 1024));
        for diff in self.diffs.iter() {
            if diff.id >= start_id {
                diffs.push(diff.clone());
            }
            if diffs.len() >= max_len {
                break;
            }
        }
        Ok(diffs)
    }

    /// Pick sstable ids of given `levels` that overlaps with given key `range`.
    /// The length of the retrun vector matches the length of the given levels.
    ///
    /// If the compaction strategy is `NonOverlap`, the retrun sstable ids of the level are
    /// guaranteed sorted in ASC order.
    async fn pick_overlap_ssts_by_user_key_range(
        &self,
        levels: Range<usize>,
        range: RangeInclusive<&[u8]>,
    ) -> Result<Vec<Vec<u64>>> {
        let mut result = vec![vec![]; levels.end - levels.start];
        let level_start = levels.start;
        for level in levels {
            let compaction_strategy = self
                .level_options
                .get(level)
                .ok_or_else(|| {
                    ManifestError::InvalidVersionDiff(format!("invalid level idx: {}", level))
                })?
                .compaction_strategy;
            for sst_id in &self.levels[level] {
                let sst = self.sstable_store.sstable(*sst_id).await?;
                if sst.is_overlap_with_user_key_range(range.clone()) {
                    result[level - level_start].push(*sst_id);
                }
                if compaction_strategy == LevelCompactionStrategy::NonOverlap
                    && &user_key(sst.first_key()) > range.end()
                {
                    break;
                }
            }
        }
        Ok(result)
    }

    /// Pick sstable ids of given `levels` that overlaps with given key. Bloom filters are supposed
    /// to be used. The length of the retrun vector matches the length of the given levels.
    ///
    /// If the compaction strategy is `NonOverlap`, the retrun sstable ids of the level are
    /// guaranteed sorted in ASC order.
    async fn pick_overlap_ssts_by_key(
        &self,
        levels: Range<usize>,
        key: &[u8],
    ) -> Result<Vec<Vec<u64>>> {
        let mut result = vec![vec![]; levels.end - levels.start];
        let level_start = levels.start;
        for level in levels {
            let compaction_strategy = self
                .level_options
                .get(level)
                .ok_or_else(|| {
                    ManifestError::InvalidVersionDiff(format!("invalid level idx: {}", level))
                })?
                .compaction_strategy;

            for sst_id in &self.levels[level] {
                let sst = self.sstable_store.sstable(*sst_id).await?;
                if sst.may_contain_key(key) && sst.is_overlap_with_user_key_range(key..=key) {
                    result[level - level_start].push(*sst_id);
                }
                if compaction_strategy == LevelCompactionStrategy::NonOverlap
                    && user_key(sst.first_key()) > key
                {
                    break;
                }
            }
        }
        Ok(result)
    }

    async fn pick_overlap_ssts_by_sst_id(
        &self,
        levels: Range<usize>,
        sst_id: u64,
    ) -> Result<Vec<Vec<u64>>> {
        let sst = self.sstable_store.sstable(sst_id).await?;
        self.pick_overlap_ssts_by_user_key_range(
            levels,
            user_key(sst.first_key())..=user_key(sst.last_key()),
        )
        .await
    }

    async fn pick_overlap_ssts_by_sst_ids(
        &self,
        levels: Range<usize>,
        sst_ids: Vec<u64>,
    ) -> Result<Vec<Vec<u64>>> {
        if sst_ids.is_empty() {
            return Ok(vec![]);
        }
        let mut first_user_key = Vec::default();
        let mut last_user_key = Vec::default();
        for sst_id in sst_ids {
            let sst = self.sstable_store.sstable(sst_id).await?;
            let sst_first_user_key = (user_key(sst.first_key())).to_vec();
            let sst_last_user_key = (user_key(sst.last_key())).to_vec();
            if first_user_key.is_empty() || sst_first_user_key < first_user_key {
                first_user_key = sst_first_user_key;
            }
            if last_user_key.is_empty() || sst_last_user_key > last_user_key {
                last_user_key = sst_last_user_key
            }
        }
        self.pick_overlap_ssts_by_user_key_range(levels, &first_user_key..=&last_user_key)
            .await
    }

    /// Verify if ASC order and non-overlap is guaranteed with non-overlap levels.
    ///
    /// Return `Ok(true)` or `Ok(false)` for verifing, `Err(_)` for other errors.
    async fn verify_non_overlap(&self) -> Result<bool> {
        for level in 0..self.level_options.len() {
            if self.level_compaction_strategy(level as u64).unwrap()
                == LevelCompactionStrategy::NonOverlap
                && self.levels[level].len() > 1
            {
                let prev_sst = self.sstable_store.sstable(self.levels[level][0]).await?;
                for sst_id in self.levels[level][1..].iter() {
                    let sst = self.sstable_store.sstable(*sst_id).await?;
                    if sst.first_key() <= prev_sst.last_key() {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}

#[derive(Clone)]
pub struct VersionManager {
    inner: Arc<RwLock<VersionManagerCore>>,
}

impl VersionManager {
    pub fn new(options: VersionManagerOptions) -> Self {
        Self {
            inner: Arc::new(RwLock::new(VersionManagerCore::new(options))),
        }
    }

    pub async fn levels(&self) -> usize {
        self.inner.read().await.levels()
    }

    pub async fn level_data_size(&self, level_idx: usize) -> usize {
        self.inner.read().await.level_data_size(level_idx)
    }

    pub async fn watermark(&self) -> u64 {
        self.inner.read().await.watermark()
    }

    pub async fn advance(&self, watermark: u64) -> Result<()> {
        self.inner.write().await.advance(watermark)
    }

    pub async fn latest_version_id(&self) -> u64 {
        self.inner.read().await.latest_version_id()
    }

    pub async fn update(&self, diff: VersionDiff, sync: bool) -> Result<()> {
        self.inner.write().await.update(diff, sync).await
    }

    /// Revoke all version diffs whose id is smaller than given `diff_id`.
    pub async fn squash(&self, diff_id: u64) {
        self.inner.write().await.squash(diff_id)
    }

    pub async fn level_compression_algorithm(
        &self,
        level_idx: u64,
    ) -> Result<CompressionAlgorithm> {
        self.inner
            .read()
            .await
            .level_compression_algorithm(level_idx)
    }

    pub async fn level_compaction_strategy(
        &self,
        level_idx: u64,
    ) -> Result<LevelCompactionStrategy> {
        self.inner.read().await.level_compaction_strategy(level_idx)
    }

    /// Get at most `max_len` version diffs from given `start_id`.
    pub async fn version_diffs_from(
        &self,
        start_id: u64,
        max_len: usize,
    ) -> Result<Vec<VersionDiff>> {
        self.inner
            .read()
            .await
            .version_diffs_from(start_id, max_len)
    }

    /// Pick sstable ids of given `levels` that overlaps with given key `range`.
    /// The length of the retrun vector matches the length of the given levels.
    ///
    /// If the compaction strategy is `NonOverlap`, the retrun sstable ids of the level are
    /// guaranteed sorted in ASC order.
    pub async fn pick_overlap_ssts(
        &self,
        levels: Range<usize>,
        range: RangeInclusive<&[u8]>,
    ) -> Result<Vec<Vec<u64>>> {
        self.inner
            .read()
            .await
            .pick_overlap_ssts_by_user_key_range(levels, range)
            .await
    }

    /// Pick sstable ids of given `levels` that overlaps with given key. Bloom filters are supposed
    /// to be used. The length of the retrun vector matches the length of the given levels.
    ///
    /// If the compaction strategy is `NonOverlap`, the retrun sstable ids of the level are
    /// guaranteed sorted in ASC order.
    pub async fn pick_overlap_ssts_by_key(
        &self,
        levels: Range<usize>,
        key: &[u8],
    ) -> Result<Vec<Vec<u64>>> {
        self.inner
            .read()
            .await
            .pick_overlap_ssts_by_key(levels, key)
            .await
    }

    pub async fn pick_overlap_ssts_by_sst_id(
        &self,
        levels: Range<usize>,
        sst_id: u64,
    ) -> Result<Vec<Vec<u64>>> {
        self.inner
            .read()
            .await
            .pick_overlap_ssts_by_sst_id(levels, sst_id)
            .await
    }

    pub async fn pick_overlap_ssts_by_sst_ids(
        &self,
        levels: Range<usize>,
        sst_ids: Vec<u64>,
    ) -> Result<Vec<Vec<u64>>> {
        self.inner
            .read()
            .await
            .pick_overlap_ssts_by_sst_ids(levels, sst_ids)
            .await
    }

    /// Verify if ASC order and non-overlap is guaranteed with non-overlap levels.
    ///
    /// Return `Ok(true)` or `Ok(false)` for verifing, `Err(_)` for other errors.
    pub async fn verify_non_overlap(&self) -> Result<bool> {
        self.inner.read().await.verify_non_overlap().await
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use itertools::Itertools;
    use runkv_proto::manifest::SstableDiff;
    use test_log::test;

    use super::*;
    use crate::lsm_tree::components::{
        BlockCache, BlockMeta, CachePolicy, Sstable, SstableBuilder, SstableBuilderOptions,
        SstableMeta, SstableStore, SstableStoreOptions,
    };
    use crate::utils::full_key;
    use crate::MemObjectStore;

    #[test(tokio::test)]
    async fn test_update_squash_version_diffs() {
        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        // Ingest sst with id {i} into level {i}.
        // All sst key range are: [b"fff"..=b"hhh"].
        version_manager.levels = (0..7).map(|i| vec![i + 1]).collect_vec();
        for i in 1..=7 {
            ingest_meta(&sstable_store, i, fkey(b"fff"), fkey(b"hhh")).await;
        }

        ingest_meta(&sstable_store, 8, fkey(b"aaa"), fkey(b"ccc")).await;
        ingest_meta(&sstable_store, 9, fkey(b"yyy"), fkey(b"zzz")).await;

        let insert_diffs = vec![
            VersionDiff {
                id: 1,
                sstable_diffs: vec![SstableDiff {
                    id: 8,
                    level: 1,
                    op: SstableOp::Insert.into(),
                    data_size: 0,
                }],
            },
            VersionDiff {
                id: 2,
                sstable_diffs: vec![SstableDiff {
                    id: 9,
                    level: 2,
                    op: SstableOp::Insert.into(),
                    data_size: 0,
                }],
            },
        ];

        for diff in &insert_diffs {
            version_manager.update(diff.clone(), false).await.unwrap()
        }

        assert_eq!(
            version_manager.levels,
            vec![
                vec![1],
                vec![8, 2],
                vec![3, 9],
                vec![4],
                vec![5],
                vec![6],
                vec![7],
            ]
        );

        let delete_diffs = vec![
            VersionDiff {
                id: 3,
                sstable_diffs: vec![SstableDiff {
                    id: 2,
                    level: 1,
                    op: SstableOp::Delete.into(),
                    data_size: 0,
                }],
            },
            VersionDiff {
                id: 4,
                sstable_diffs: vec![SstableDiff {
                    id: 3,
                    level: 2,
                    op: SstableOp::Delete.into(),
                    data_size: 0,
                }],
            },
        ];

        for diff in &delete_diffs {
            version_manager.update(diff.clone(), false).await.unwrap()
        }

        assert_eq!(
            version_manager.levels,
            vec![
                vec![1],
                vec![8],
                vec![9],
                vec![4],
                vec![5],
                vec![6],
                vec![7],
            ]
        );

        assert_eq!(
            version_manager.version_diffs_from(1, 10).unwrap(),
            [insert_diffs.clone(), delete_diffs.clone()].concat()
        );

        version_manager.squash(3);

        assert!(version_manager.version_diffs_from(1, 10).is_err());

        assert_eq!(
            version_manager.version_diffs_from(3, 10).unwrap(),
            delete_diffs.clone()
        );

        assert!(version_manager
            .update(
                VersionDiff {
                    id: 10,
                    sstable_diffs: vec![SstableDiff {
                        id: 10,
                        level: 1,
                        op: SstableOp::Insert.into(),
                        data_size: 0,
                    }],
                },
                false
            )
            .await
            .is_err());

        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![],
            vec![1, 2, 3],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        ];
        ingest_meta(&sstable_store, 1, fkey(b"aaa"), fkey(b"bbb")).await;
        ingest_meta(&sstable_store, 2, fkey(b"ccc"), fkey(b"ddd")).await;
        ingest_meta(&sstable_store, 3, fkey(b"eee"), fkey(b"fff")).await;
        ingest_meta(&sstable_store, 4, fkey(b"abb"), fkey(b"bdd")).await;
        assert_matches!(
            version_manager
                .update(
                    VersionDiff {
                        id: 1,
                        sstable_diffs: vec![SstableDiff {
                            id: 4,
                            level: 1,
                            op: SstableOp::Insert.into(),
                            data_size: 0,
                        }],
                    },
                    false
                )
                .await,
            Err(crate::Error::ManifestError(
                ManifestError::InvalidVersionDiff(_)
            ))
        )
    }

    #[test(tokio::test)]
    async fn test_pick_overlap_ssts() {
        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![1, 2, 3],
            vec![4],
            vec![],
            vec![5],
            vec![6, 7],
            vec![],
            vec![],
        ];
        ingest_meta(&sstable_store, 1, fkey(b"aaa"), fkey(b"fff")).await;
        ingest_meta(&sstable_store, 2, fkey(b"bbb"), fkey(b"ggg")).await;
        ingest_meta(&sstable_store, 3, fkey(b"xxx"), fkey(b"zzz")).await;
        ingest_meta(&sstable_store, 4, fkey(b"bbb"), fkey(b"fff")).await;
        ingest_meta(&sstable_store, 5, fkey(b"xxx"), fkey(b"zzz")).await;
        ingest_meta(&sstable_store, 6, fkey(b"aaa"), fkey(b"ddd")).await;
        ingest_meta(&sstable_store, 7, fkey(b"eee"), fkey(b"fff")).await;
        assert_eq!(
            version_manager
                .pick_overlap_ssts_by_user_key_range(0..7, &fkey(b"eee")[..]..=&fkey(b"fff")[..])
                .await
                .unwrap(),
            vec![vec![1, 2], vec![4], vec![], vec![], vec![7], vec![], vec![]]
        );
    }

    #[test(tokio::test)]
    async fn test_pick_overlap_ssts_by_key() {
        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![1, 2, 3],
            vec![],
            vec![],
            vec![4],
            vec![5, 6],
            vec![],
            vec![],
        ];
        build_and_ingest_sst(
            sstable_store.clone(),
            1,
            &[(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")],
            4,
        )
        .await;
        build_and_ingest_sst(
            sstable_store.clone(),
            2,
            &[(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")],
            3,
        )
        .await;
        build_and_ingest_sst(
            sstable_store.clone(),
            3,
            &[(b"k7", b"v7"), (b"k8", b"v8"), (b"k9", b"v9")],
            4,
        )
        .await;
        build_and_ingest_sst(
            sstable_store.clone(),
            4,
            &[(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")],
            2,
        )
        .await;
        build_and_ingest_sst(
            sstable_store.clone(),
            5,
            &[(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")],
            1,
        )
        .await;
        build_and_ingest_sst(
            sstable_store.clone(),
            6,
            &[(b"k7", b"v7"), (b"k8", b"v8"), (b"k9", b"v9")],
            1,
        )
        .await;

        assert_eq!(
            version_manager
                .pick_overlap_ssts_by_key(0..7, &key(b"k2"))
                .await
                .unwrap(),
            vec![vec![1, 2], vec![], vec![], vec![4], vec![5], vec![], vec![]]
        );
    }

    #[test(tokio::test)]
    async fn test_verify_non_overlap() {
        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![],
            vec![1, 2, 3],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        ];
        ingest_meta(&sstable_store, 1, fkey(b"aaa"), fkey(b"bbb")).await;
        ingest_meta(&sstable_store, 2, fkey(b"ccc"), fkey(b"ddd")).await;
        ingest_meta(&sstable_store, 3, fkey(b"eee"), fkey(b"fff")).await;
        assert!(version_manager.verify_non_overlap().await.unwrap());

        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![],
            vec![1, 2, 3],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        ];
        ingest_meta(&sstable_store, 1, fkey(b"aaa"), fkey(b"fff")).await;
        ingest_meta(&sstable_store, 2, fkey(b"bbb"), fkey(b"ggg")).await;
        ingest_meta(&sstable_store, 3, fkey(b"ccc"), fkey(b"hhh")).await;
        assert!(!version_manager.verify_non_overlap().await.unwrap());

        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![],
            vec![1, 2, 3],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        ];
        ingest_meta(&sstable_store, 1, fkey(b"eee"), fkey(b"fff")).await;
        ingest_meta(&sstable_store, 2, fkey(b"ccc"), fkey(b"ddd")).await;
        ingest_meta(&sstable_store, 3, fkey(b"aaa"), fkey(b"bbb")).await;
        assert!(!version_manager.verify_non_overlap().await.unwrap());

        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        version_manager.levels = vec![
            vec![],
            vec![1, 2, 3],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        ];
        ingest_meta(&sstable_store, 3, fkey(b"ccc"), fkey(b"hhh")).await;
        ingest_meta(&sstable_store, 2, fkey(b"bbb"), fkey(b"ggg")).await;
        ingest_meta(&sstable_store, 1, fkey(b"aaa"), fkey(b"fff")).await;
        assert!(!version_manager.verify_non_overlap().await.unwrap());
    }

    async fn ingest_meta(
        sstable_store: &SstableStoreRef,
        sst_id: u64,
        first_key: Vec<u8>,
        last_key: Vec<u8>,
    ) {
        sstable_store
            .put(
                &Sstable::new(
                    sst_id,
                    Arc::new(SstableMeta {
                        block_metas: vec![BlockMeta {
                            offset: 0,
                            len: 0,
                            first_key,
                            last_key,
                        }],
                        bloom_filter_bytes: vec![],
                        data_size: 0,
                    }),
                ),
                Vec::default(),
                // Disable block cache, inserting block cache need to decode block data, which is
                // empty for test.
                CachePolicy::Disable,
            )
            .await
            .unwrap();
    }

    async fn build_and_ingest_sst(
        sstable_store: SstableStoreRef,
        sst_id: u64,
        kvs: &[(&'static [u8], &'static [u8])],
        sequence: u64,
    ) {
        let options = SstableBuilderOptions::default();
        let mut builder = SstableBuilder::new(options);
        for (k, v) in kvs {
            builder.add(k, sequence, Some(v)).unwrap();
        }
        let (meta, data) = builder.build().unwrap();
        let sst = Sstable::new(sst_id, Arc::new(meta));
        sstable_store
            .put(&sst, data, CachePolicy::Disable)
            .await
            .unwrap();
    }

    fn build_sstable_store_for_test() -> SstableStoreRef {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(0, 0);
        let sstable_store_options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 65536,
        };
        Arc::new(SstableStore::new(sstable_store_options))
    }

    fn build_version_manager_for_test(sstable_store: SstableStoreRef) -> VersionManagerCore {
        let level_options = vec![
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::Overlap,
                compression_algorithm: CompressionAlgorithm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorithm: CompressionAlgorithm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorithm: CompressionAlgorithm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorithm: CompressionAlgorithm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorithm: CompressionAlgorithm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorithm: CompressionAlgorithm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorithm: CompressionAlgorithm::Lz4,
            },
        ];
        let version_manager_options = VersionManagerOptions {
            levels_options: level_options,
            levels: vec![vec![]; 7],
            sstable_store,
        };
        VersionManagerCore::new(version_manager_options)
    }

    fn fkey(s: &'static [u8]) -> Vec<u8> {
        full_key(s, 1)
    }

    fn key(s: &'static [u8]) -> Vec<u8> {
        s.to_vec()
    }
}
