use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytesize::ByteSize;
use futures::future;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::{thread_rng, Rng};
use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::{LevelCompactionStrategy, LevelOptions};
use runkv_common::Worker;
use runkv_proto::exhauster::exhauster_service_client::ExhausterServiceClient;
use runkv_proto::exhauster::CompactionRequest;
use runkv_proto::manifest::{SstableDiff, SstableOp, VersionDiff};
use runkv_proto::meta::KeyRange;
use runkv_storage::manifest::VersionManager;
use tonic::Request;
use tracing::{error, trace, warn};

use crate::error::{Error, Result};
use crate::meta::MetaStoreRef;

#[derive(Clone, Debug)]
pub struct LsmTreeConfig {
    pub trigger_l0_compaction_ssts: usize,
    pub trigger_l0_compaction_interval: Duration,
    pub trigger_lmax_compaction_interval: Duration,
    pub trigger_compaction_interval: Duration,

    pub l1_capacity: usize,
    pub level_multiplier: usize,

    pub sstable_capacity: usize,
    pub block_capacity: usize,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,

    pub compaction_pin_ttl: Duration,

    pub levels_options: Vec<LevelOptions>,
}

impl TryFrom<runkv_common::config::LsmTreeConfig> for LsmTreeConfig {
    type Error = Error;

    fn try_from(
        cfg: runkv_common::config::LsmTreeConfig,
    ) -> core::result::Result<Self, Self::Error> {
        Ok(Self {
            trigger_l0_compaction_ssts: cfg.trigger_l0_compaction_ssts,
            trigger_l0_compaction_interval: cfg
                .trigger_l0_compaction_interval
                .parse::<humantime::Duration>()
                .map_err(Error::config_err)?
                .into(),
            trigger_lmax_compaction_interval: cfg
                .trigger_lmax_compaction_interval
                .parse::<humantime::Duration>()
                .map_err(Error::config_err)?
                .into(),
            trigger_compaction_interval: cfg
                .trigger_compaction_interval
                .parse::<humantime::Duration>()
                .map_err(Error::config_err)?
                .into(),
            l1_capacity: cfg
                .l1_capacity
                .parse::<ByteSize>()
                .map_err(Error::config_err)?
                .0 as usize,
            level_multiplier: cfg.level_multiplier,
            sstable_capacity: cfg
                .sstable_capacity
                .parse::<ByteSize>()
                .map_err(Error::config_err)?
                .0 as usize,
            block_capacity: cfg
                .block_capacity
                .parse::<ByteSize>()
                .map_err(Error::config_err)?
                .0 as usize,
            restart_interval: cfg.restart_interval,
            bloom_false_positive: cfg.bloom_false_positive,
            compaction_pin_ttl: cfg
                .compaction_pin_ttl
                .parse::<humantime::Duration>()
                .map_err(Error::config_err)?
                .into(),
            levels_options: cfg.levels_options,
        })
    }
}

pub struct CompactionDetectorOptions {
    pub meta_store: MetaStoreRef,
    pub version_manager: VersionManager,
    pub channel_pool: ChannelPool,

    pub lsm_tree_config: LsmTreeConfig,

    pub health_timeout: Duration,
}

#[derive(Clone)]
struct CompactionContext {
    level: u64,
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,
    lsm_tree_config: LsmTreeConfig,
    health_timeout: Duration,
}

#[derive(Default, Clone)]
struct TwoLevelSstables {
    first: Vec<u64>,
    second: Vec<u64>,
}

impl TwoLevelSstables {
    fn to_vec(&self) -> Vec<u64> {
        self.first
            .iter()
            .chain(self.second.iter())
            .copied()
            .collect_vec()
    }
}

type PartitionPoint = Vec<u8>;

pub struct CompactionDetector {
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,

    lsm_tree_config: LsmTreeConfig,

    health_timeout: Duration,
}

#[async_trait]
impl Worker for CompactionDetector {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => {
                    warn!("error occur when compactor running: {}", e);
                }
            }
        }
    }
}

impl CompactionDetector {
    pub fn new(options: CompactionDetectorOptions) -> Self {
        Self {
            version_manager: options.version_manager,
            meta_store: options.meta_store,
            channel_pool: options.channel_pool,

            lsm_tree_config: options.lsm_tree_config,

            health_timeout: options.health_timeout,
        }
    }

    fn create_context(&self, level: u64) -> CompactionContext {
        CompactionContext {
            level,
            meta_store: self.meta_store.clone(),
            version_manager: self.version_manager.clone(),
            channel_pool: self.channel_pool.clone(),
            lsm_tree_config: self.lsm_tree_config.clone(),
            health_timeout: self.health_timeout,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        let mut trigger_l0_ticker =
            tokio::time::interval(self.lsm_tree_config.trigger_l0_compaction_interval);
        let mut trigger_lmax_ticker =
            tokio::time::interval(self.lsm_tree_config.trigger_lmax_compaction_interval);
        let mut trigger_ticker =
            tokio::time::interval(self.lsm_tree_config.trigger_compaction_interval);
        let lmax = self.lsm_tree_config.levels_options.len() as u64 - 1;

        loop {
            tokio::select! {
                _ = trigger_l0_ticker.tick() => {
                    trace!("tick l0 compaction [interval: {:?}]", self.lsm_tree_config.trigger_l0_compaction_interval);
                    let ctx = self.create_context(0);
                    tokio::spawn(
                        async move {
                            if let Err(e) = trigger_compaction(ctx).await {
                                error!("trigger compaction l0 error: {}", e);
                            }
                        }
                    );
                }
                _ = trigger_lmax_ticker.tick() => {
                    trace!("tick lmax compaction [interval: {:?}]", self.lsm_tree_config.trigger_lmax_compaction_interval);
                    let ctx = self.create_context(lmax);
                    tokio::spawn(
                        async move {
                            if let Err(e) = trigger_compaction(ctx).await {
                                error!("trigger compaction lmax error: {}", e);
                            }
                        }
                    );
                }
                _ = trigger_ticker.tick() => {
                    trace!("tick compaction [interval: {:?}]", self.lsm_tree_config.trigger_compaction_interval);
                    match check_levels_size(
                        self.version_manager.clone(),
                        self.lsm_tree_config.l1_capacity,
                        self.lsm_tree_config.level_multiplier,
                    ).await {
                        Ok(levels) => {
                            for (level_idx, overstep) in levels.into_iter().enumerate() {
                                if !overstep {
                                    continue;
                                }
                                let ctx = self.create_context(level_idx as u64);
                                tokio::spawn(
                                    async move {
                                        if let Err(e) = trigger_compaction(ctx).await {
                                            error!("trigger compaction l{} error: {}", level_idx, e);
                                        }
                                    }
                                );
                            }
                        },
                        Err(e) => {
                            error!("check levels size error: {}", e);
                        }
                    }
                }
            }
        }
    }
}

/// Check if there is level whose size overstep the level size limit and returns the level indices.
///
/// Note: L0 and Lmax will always be false.
async fn check_levels_size(
    version_manager: VersionManager,
    l1_capacity: usize,
    level_multiplier: usize,
) -> Result<Vec<bool>> {
    let levels = version_manager.levels().await;
    let mut overstep = Vec::with_capacity(levels);
    overstep.push(false);
    let mut limit = l1_capacity;
    for level_idx in 1..levels - 1 {
        let level_data_size = version_manager.level_data_size(level_idx).await;
        trace!(
            "check level size: [level idx: {}] [size: {}] [capacity: {}]",
            level_idx,
            level_data_size,
            limit
        );
        overstep.push(level_data_size > limit);
        limit *= level_multiplier;
    }
    overstep.push(false);
    Ok(overstep)
}

/// Trigger compaction job based on input [`CompactionContext`].
async fn trigger_compaction(ctx: CompactionContext) -> Result<()> {
    let now = SystemTime::now();

    // Calculate partition points based on node ranges.
    let node_ranges = ctx.meta_store.all_node_ranges().await?;
    let partition_points = node_ranges
        .iter()
        .flat_map(|(_node_id, ranges)| ranges.iter().map(|range| range.start_key.clone()))
        .collect_vec();

    // Pick sstables to compact, grouped by node.
    let old_node_ssts = pick_ssts(&ctx, node_ranges, now).await?;

    // All sstables to compact.
    let old_ssts = old_node_ssts
        .iter()
        .flat_map(|(_node_id, old_ssts)| old_ssts.to_vec())
        .collect_vec();
    if old_ssts.is_empty() {
        return Ok(());
    }
    assert!(verify_no_duplication(old_ssts.iter()));

    // Collect sub compaction jobs to exhausters.
    let mut futures = Vec::with_capacity(old_node_ssts.len());
    for (_node_id, old_ssts) in old_node_ssts.clone() {
        let sub_ctx = ctx.clone();
        let partition_points_clone = partition_points.clone();
        let future = async move {
            if !sub_ctx
                .meta_store
                .pin_sstables(&old_ssts.to_vec(), now)
                .await?
            {
                warn!("some sstable has been pinned, skip compaction");
                return Ok(());
            }

            sub_compaction(&sub_ctx, old_ssts.clone(), partition_points_clone, now).await?;

            if let Err(e) = sub_ctx.meta_store.unpin_sstables(&old_ssts.to_vec()).await {
                error!("failed to unpin sstables, will be resolved by timeout");
                return Err(e);
            }
            Ok(())
        };
        futures.push(future);
    }

    // Distribute sub compaction jobs to exhausters and collect results.
    let results = future::join_all(futures).await;
    let errs = results.into_iter().filter(|r| r.is_err()).collect_vec();
    if !errs.is_empty() {
        return Err(Error::Other(format!("compaction error: {:?}", errs)));
    }
    Ok(())
}

/// Build compaction request, collect response, update version manager.
///
/// # Safety
///
/// Pin sstable before `sub_compaction` and unpin after it.
async fn sub_compaction(
    ctx: &CompactionContext,
    old_ssts: TwoLevelSstables,
    partition_points: Vec<PartitionPoint>,
    now: SystemTime,
) -> Result<()> {
    let watermark = ctx.version_manager.watermark().await;

    let target_level = if ctx.level as usize + 1 == ctx.lsm_tree_config.levels_options.len() {
        ctx.level
    } else {
        ctx.level + 1
    };

    let req = CompactionRequest {
        sst_ids: old_ssts.to_vec(),
        watermark,
        sstable_capacity: ctx.lsm_tree_config.sstable_capacity as u64,
        block_capacity: ctx.lsm_tree_config.block_capacity as u64,
        restart_interval: ctx.lsm_tree_config.restart_interval as u64,
        bloom_false_positive: ctx.lsm_tree_config.bloom_false_positive,
        compression_algorithm: ctx
            .lsm_tree_config
            .levels_options
            .get(target_level as usize)
            .unwrap_or_else(|| panic!("no config for {}", ctx.level as usize + 1))
            .compression_algorithm
            .into(),
        remove_tombstone: target_level as usize == ctx.lsm_tree_config.levels_options.len() - 1,
        partition_points: partition_points.clone(),
    };

    let exhauster = ctx.meta_store.pick_exhauster(ctx.health_timeout).await?;
    if exhauster.is_none() {
        warn!("no live exhauster, skip compaction");
        return Ok(());
    }
    let exhauster = exhauster.unwrap();

    let mut client =
        ExhausterServiceClient::new(ctx.channel_pool.get(exhauster).await.map_err(Error::err)?);
    let rsp = client.compaction(Request::new(req)).await?.into_inner();

    if now.elapsed().unwrap() > ctx.lsm_tree_config.compaction_pin_ttl {
        warn!("compaction timeout, skip version updates");
        return Ok(());
    }

    let new_sst_infos = rsp.new_sst_infos;
    let old_sst_sizes = rsp
        .old_sst_infos
        .iter()
        .map(|sst_info| (sst_info.id, sst_info.data_size))
        .collect::<BTreeMap<u64, u64>>();

    let mut sstable_diffs =
        Vec::with_capacity(old_ssts.first.len() + old_ssts.second.len() + new_sst_infos.len());

    for sst_id in old_ssts.first.iter() {
        sstable_diffs.push(SstableDiff {
            id: *sst_id,
            level: ctx.level,
            op: SstableOp::Delete.into(),
            data_size: *old_sst_sizes.get(sst_id).expect("old sst size not found"),
        });
    }
    for sst_id in old_ssts.second.iter() {
        sstable_diffs.push(SstableDiff {
            id: *sst_id,
            level: ctx.level + 1,
            op: SstableOp::Delete.into(),
            data_size: *old_sst_sizes.get(sst_id).expect("old sst size not found"),
        });
    }
    for sst_info in new_sst_infos.iter() {
        sstable_diffs.push(SstableDiff {
            id: sst_info.id,
            level: target_level,
            op: SstableOp::Insert.into(),
            data_size: sst_info.data_size,
        });
    }

    let version_diff = VersionDiff {
        id: 0,
        sstable_diffs,
    };
    trace!("compaction version diff:\n{:#?}", version_diff);
    ctx.version_manager.update(version_diff, false).await?;

    Ok(())
}

/// Pick sstables to compact.
async fn pick_ssts(
    ctx: &CompactionContext,
    node_ranges: BTreeMap<u64, Vec<KeyRange>>,
    now: SystemTime,
) -> Result<BTreeMap<u64, TwoLevelSstables>> {
    let mut old_node_ssts = BTreeMap::default();

    for (node_id, ranges) in node_ranges {
        let mut base_level_ssts: BTreeSet<u64> = BTreeSet::default();
        let mut next_level_ssts: BTreeSet<u64> = BTreeSet::default();

        // TODO: Check ranges in random order.
        for range in ranges {
            let base_range_ssts = ctx
                .version_manager
                .pick_overlap_ssts(
                    ctx.level as usize..ctx.level as usize + 1,
                    &range.start_key[..]..=&range.end_key[..],
                )
                .await?;
            assert_eq!(base_range_ssts.len(), 1);
            if base_range_ssts[0].is_empty() {
                // Skip if there is no sst to compact.
                continue;
            }
            if ctx.level == 0 {
                // L0 compaction always involves all L0 sstables of a node.
                base_level_ssts.extend(base_range_ssts[0].iter());
                continue;
            }
            match ctx.lsm_tree_config.levels_options[ctx.level as usize].compaction_strategy {
                LevelCompactionStrategy::NonOverlap => {
                    // TODO: Better strategy.
                    // TODO: Make the ratio configurable.
                    // Involve at most 10% sstables.
                    let base_range_sst_count = base_range_ssts[0].len();
                    let involves = std::cmp::max(base_range_sst_count / 10, 1);
                    let first_idx = thread_rng().gen_range(0..=base_range_sst_count - involves);
                    base_level_ssts
                        .extend(base_range_ssts[0][first_idx..first_idx + involves].iter());
                }
                LevelCompactionStrategy::Overlap => {
                    // TODO: Better strategy.
                    // Random pick 1% sstables and overlap sstables with them.
                    let base_range_sst_count = base_range_ssts[0].len();
                    let involves = std::cmp::max(base_range_sst_count / 100, 1);
                    let mut base_level_ssts_set: BTreeSet<u64> = BTreeSet::default();
                    let ssts = base_range_ssts[0]
                        .choose_multiple(&mut thread_rng(), involves)
                        .copied()
                        .collect_vec();
                    base_level_ssts_set.extend(ssts.iter());
                    for sst in ssts {
                        let overlaps = ctx
                            .version_manager
                            .pick_overlap_ssts_by_sst_id(
                                ctx.level as usize..ctx.level as usize + 1,
                                sst,
                            )
                            .await?;
                        assert_eq!(overlaps.len(), 1);
                        base_level_ssts_set.extend(overlaps[0].iter());
                    }
                    base_level_ssts.extend(base_level_ssts_set.iter());
                }
            }
            // TODO: Stop picking if there is already too many.
        }
        if ctx.level == 0 && base_level_ssts.len() < ctx.lsm_tree_config.trigger_l0_compaction_ssts
        {
            // Skip if not enough sstable involved for L0 compaction.
            continue;
        }
        // Pick overlapping sstable in `level + 1` iff compaction strategy of `level + 1` is
        // `NonOverlap`.
        if ctx.level as usize + 1 < ctx.lsm_tree_config.levels_options.len()
            && ctx.lsm_tree_config.levels_options[ctx.level as usize + 1].compaction_strategy
                == LevelCompactionStrategy::NonOverlap
        {
            let next_ssts = ctx
                .version_manager
                .pick_overlap_ssts_by_sst_ids(
                    ctx.level as usize + 1..ctx.level as usize + 2,
                    base_level_ssts.iter().cloned().collect_vec(),
                )
                .await?;
            assert_eq!(next_ssts.len(), 1);
            next_level_ssts.extend(next_ssts[0].iter());
        }
        let base_level_ssts = base_level_ssts.into_iter().collect_vec();
        let next_level_ssts = next_level_ssts.into_iter().collect_vec();

        if ctx
            .meta_store
            .is_sstables_pinned(&base_level_ssts, now)
            .await?
            .iter()
            .chain(
                ctx.meta_store
                    .is_sstables_pinned(&next_level_ssts, now)
                    .await?
                    .iter(),
            )
            .any(|pinned| *pinned)
        {
            continue;
        }

        old_node_ssts.insert(
            node_id,
            TwoLevelSstables {
                first: base_level_ssts,
                second: next_level_ssts,
            },
        );
    }
    Ok(old_node_ssts)
}

fn verify_no_duplication(mut iter: core::slice::Iter<u64>) -> bool {
    let mut unique = HashSet::new();
    iter.all(|v| unique.insert(*v))
}
