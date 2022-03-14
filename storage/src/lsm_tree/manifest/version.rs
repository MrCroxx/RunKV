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
    /// Initial sst ids of each level.
    ///
    /// If the compaction strategy is `NonOverlap`, the sstable ids of the level must be guaranteed
    /// sorted in ASC order.
    pub levels: Vec<Vec<u64>>,
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
    ///
    /// TODO: Restore diff from `MetaStore`.
    diffs: LinkedList<VersionDiff>,
    /// `sstable_store` is used to fetch sstable meta.
    sstable_store: SstableStoreRef,
}

impl VersionManager {
    pub fn new(options: VersionManagerOptions) -> Result<Self> {
        if options.levels.len() != options.level_options.len() {
            return Err(
                ManifestError::Other("level.len() != level_options.len()".to_string()).into(),
            );
        }
        Ok(Self {
            level_options: options.level_options,
            levels: options.levels,
            diffs: LinkedList::default(),
            sstable_store: options.sstable_store,
        })
    }

    pub async fn update(&mut self, diff: VersionDiff) -> Result<()> {
        let current_diff_id = self.diffs.back().map(|diff| diff.id).unwrap_or_else(|| 0);
        if !self.diffs.is_empty() && current_diff_id + 1 != diff.id {
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
    pub fn squash(&mut self, diff_id: u64) {
        while self
            .diffs
            .front()
            .as_ref()
            .map_or_else(|| false, |diff| diff.id < diff_id)
        {
            self.diffs.pop_front();
        }
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

    /// Get at most `max_len` version diffs from given `start_id`.
    pub fn version_diffs_from(&self, start_id: u64, max_len: usize) -> Result<Vec<VersionDiff>> {
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

    /// Pick sstable ids of given `levels` that overlaps with given key. Bloom filters are supposed
    /// to be used. The length of the retrun vector matches the length of the given levels.
    ///
    /// If the compaction strategy is `NonOverlap`, the retrun sstable ids of the level are
    /// guaranteed sorted in ASC order.
    pub async fn pick_overlap_ssts_by_key(
        &self,
        _levels: Range<usize>,
        _key: &Bytes,
    ) -> Result<Vec<Vec<u64>>> {
        todo!()
    }

    /// Verify if ASC order and non-overlap is guaranteed with non-overlap levels.
    ///
    /// Return `Ok(true)` or `Ok(false)` for verifing, `Err(_)` for other errors.
    pub async fn verify_non_overlap(&self) -> Result<bool> {
        for level in 0..self.level_options.len() {
            if self.level_compaction_strategy(level as u64).unwrap()
                == LevelCompactionStrategy::NonOverlap
                && self.levels[level].len() > 1
            {
                let last_meta = self.sstable_store.meta(self.levels[level][0]).await?;
                for sst_id in self.levels[level][1..].iter() {
                    let meta = self.sstable_store.meta(*sst_id).await?;
                    if meta.block_metas.first().as_ref().unwrap().first_key
                        <= last_meta.block_metas.last().as_ref().unwrap().last_key
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use itertools::Itertools;
    use runkv_proto::manifest::SsTableDiff;

    use super::*;
    use crate::{
        BlockCache, BlockMeta, CachePolicy, MemObjectStore, Sstable, SstableMeta, SstableStore,
        SstableStoreOptions,
    };

    #[tokio::test]
    async fn test_update_squash_version_diffs() {
        let sstable_store = build_sstable_store_for_test();
        let mut version_manager = build_version_manager_for_test(sstable_store.clone());
        // Ingest sst with id {i} into level {i}.
        // All sst key range are: [b"fff"..=b"hhh"].
        version_manager.levels = (0..7).map(|i| vec![i + 1]).collect_vec();
        for i in 1..=7 {
            ingest_meta(&sstable_store, i, key(b"fff"), key(b"hhh")).await;
        }

        ingest_meta(&sstable_store, 8, key(b"aaa"), key(b"ccc")).await;
        ingest_meta(&sstable_store, 9, key(b"yyy"), key(b"zzz")).await;

        let insert_diffs = vec![
            VersionDiff {
                id: 1,
                sstable_diffs: vec![SsTableDiff {
                    id: 8,
                    level: 1,
                    op: SsTableOp::Insert.into(),
                }],
            },
            VersionDiff {
                id: 2,
                sstable_diffs: vec![SsTableDiff {
                    id: 9,
                    level: 2,
                    op: SsTableOp::Insert.into(),
                }],
            },
        ];

        for diff in &insert_diffs {
            version_manager.update(diff.clone()).await.unwrap()
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
                sstable_diffs: vec![SsTableDiff {
                    id: 2,
                    level: 1,
                    op: SsTableOp::Delete.into(),
                }],
            },
            VersionDiff {
                id: 4,
                sstable_diffs: vec![SsTableDiff {
                    id: 3,
                    level: 2,
                    op: SsTableOp::Delete.into(),
                }],
            },
        ];

        for diff in &delete_diffs {
            version_manager.update(diff.clone()).await.unwrap()
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
            .update(VersionDiff {
                id: 10,
                sstable_diffs: vec![SsTableDiff {
                    id: 10,
                    level: 1,
                    op: SsTableOp::Insert.into(),
                }],
            })
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
        ingest_meta(&sstable_store, 1, key(b"aaa"), key(b"bbb")).await;
        ingest_meta(&sstable_store, 2, key(b"ccc"), key(b"ddd")).await;
        ingest_meta(&sstable_store, 3, key(b"eee"), key(b"fff")).await;
        ingest_meta(&sstable_store, 4, key(b"abb"), key(b"bdd")).await;
        assert_matches!(
            version_manager
                .update(VersionDiff {
                    id: 1,
                    sstable_diffs: vec![SsTableDiff {
                        id: 4,
                        level: 1,
                        op: SsTableOp::Insert.into(),
                    }],
                })
                .await,
            Err(crate::Error::ManifestError(
                ManifestError::InvalidVersionDiff(_)
            ))
        )
    }

    #[tokio::test]
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
        ingest_meta(&sstable_store, 1, key(b"aaa"), key(b"fff")).await;
        ingest_meta(&sstable_store, 2, key(b"bbb"), key(b"ggg")).await;
        ingest_meta(&sstable_store, 3, key(b"xxx"), key(b"zzz")).await;
        ingest_meta(&sstable_store, 4, key(b"bbb"), key(b"fff")).await;
        ingest_meta(&sstable_store, 5, key(b"xxx"), key(b"zzz")).await;
        ingest_meta(&sstable_store, 6, key(b"aaa"), key(b"ddd")).await;
        ingest_meta(&sstable_store, 7, key(b"eee"), key(b"fff")).await;
        assert_eq!(
            version_manager
                .pick_overlap_ssts(0..7, &key(b"eee")..=&key(b"fff"))
                .await
                .unwrap(),
            vec![vec![1, 2], vec![4], vec![], vec![], vec![7], vec![], vec![]]
        );
    }

    #[tokio::test]
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
        ingest_meta(&sstable_store, 1, key(b"aaa"), key(b"bbb")).await;
        ingest_meta(&sstable_store, 2, key(b"ccc"), key(b"ddd")).await;
        ingest_meta(&sstable_store, 3, key(b"eee"), key(b"fff")).await;
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
        ingest_meta(&sstable_store, 1, key(b"aaa"), key(b"fff")).await;
        ingest_meta(&sstable_store, 2, key(b"bbb"), key(b"ggg")).await;
        ingest_meta(&sstable_store, 3, key(b"ccc"), key(b"hhh")).await;
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
        ingest_meta(&sstable_store, 1, key(b"eee"), key(b"fff")).await;
        ingest_meta(&sstable_store, 2, key(b"ccc"), key(b"ddd")).await;
        ingest_meta(&sstable_store, 3, key(b"aaa"), key(b"bbb")).await;
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
        ingest_meta(&sstable_store, 3, key(b"ccc"), key(b"hhh")).await;
        ingest_meta(&sstable_store, 2, key(b"bbb"), key(b"ggg")).await;
        ingest_meta(&sstable_store, 1, key(b"aaa"), key(b"fff")).await;
        assert!(!version_manager.verify_non_overlap().await.unwrap());
    }

    async fn ingest_meta(
        sstable_store: &SstableStoreRef,
        sst_id: u64,
        first_key: Bytes,
        last_key: Bytes,
    ) {
        sstable_store
            .put(
                &Sstable {
                    id: sst_id,
                    meta: SstableMeta {
                        block_metas: vec![BlockMeta {
                            offset: 0,
                            len: 0,
                            first_key,
                            last_key,
                        }],
                        bloom_filter: vec![],
                    },
                },
                Bytes::default(),
                // Disable block cache, inserting block cache need to decode block data, which is
                // empty for test.
                CachePolicy::Disable,
            )
            .await
            .unwrap();
    }

    fn build_sstable_store_for_test() -> SstableStoreRef {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(0);
        let sstable_store_options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 65536,
        };
        Arc::new(SstableStore::new(sstable_store_options))
    }

    fn build_version_manager_for_test(sstable_store: SstableStoreRef) -> VersionManager {
        let level_options = vec![
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::Overlap,
                compression_algorighm: CompressionAlgorighm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::None,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
            LevelOptions {
                compaction_strategy: LevelCompactionStrategy::NonOverlap,
                compression_algorighm: CompressionAlgorighm::Lz4,
            },
        ];
        let version_manager_options = VersionManagerOptions {
            level_options,
            levels: vec![vec![]; 7],
            sstable_store,
        };
        VersionManager::new(version_manager_options).unwrap()
    }

    fn key(s: &'static [u8]) -> Bytes {
        Bytes::from_static(s)
    }
}
