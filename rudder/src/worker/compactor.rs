use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::future;
use itertools::Itertools;
use runkv_common::config::LevelOptions;
use runkv_common::Worker;
use runkv_proto::exhauster::exhauster_service_client::ExhausterServiceClient;
use runkv_proto::exhauster::CompactionRequest;
use runkv_proto::manifest::{SsTableDiff, SsTableOp, VersionDiff};
use runkv_proto::meta::KeyRange;
use runkv_storage::manifest::VersionManager;
use tonic::Request;
use tracing::{error, trace, warn};

use crate::error::{Error, Result};
use crate::meta::MetaStoreRef;

pub struct CompactorOptions {
    pub meta_store: MetaStoreRef,
    pub version_manager: VersionManager,

    pub trigger_l0_compaction_ssts: usize,
    pub trigger_l0_compaction_interval: Duration,
    pub trigger_compaction_interval: Duration,
    pub sstable_capacity: usize,
    pub block_capacity: usize,
    pub restart_interval: usize,
    pub bloom_false_positive: f64,
    pub levels_options: Vec<LevelOptions>,

    pub compaction_pin_ttl: Duration,
    pub health_timeout: Duration,
}

#[derive(Clone)]
struct CompactionContext {
    level: u64,
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
    trigger_l0_compaction_ssts: usize,
    sstable_capacity: usize,
    block_capacity: usize,
    restart_interval: usize,
    bloom_false_positive: f64,
    levels_options: Vec<LevelOptions>,
    compaction_pin_ttl: Duration,
    health_timeout: Duration,
}

#[derive(Clone)]
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

pub struct Compactor {
    meta_store: MetaStoreRef,
    version_manager: VersionManager,

    trigger_l0_compaction_ssts: usize,
    trigger_l0_compaction_interval: Duration,
    _trigger_compaction_interval: Duration,
    sstable_capacity: usize,
    block_capacity: usize,
    restart_interval: usize,
    bloom_false_positive: f64,
    levels_options: Vec<LevelOptions>,

    compaction_pin_ttl: Duration,
    health_timeout: Duration,
}

#[async_trait]
impl Worker for Compactor {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => {}
                Err(e) => {
                    println!("error occur when compactor running: {}", e);
                    warn!("error occur when compactor running: {}", e);
                }
            }
        }
    }
}

impl Compactor {
    pub fn new(options: CompactorOptions) -> Self {
        Self {
            version_manager: options.version_manager,
            meta_store: options.meta_store,

            trigger_l0_compaction_ssts: options.trigger_l0_compaction_ssts,
            trigger_l0_compaction_interval: options.trigger_l0_compaction_interval,
            _trigger_compaction_interval: options.trigger_compaction_interval,
            sstable_capacity: options.sstable_capacity,
            block_capacity: options.block_capacity,
            restart_interval: options.restart_interval,
            bloom_false_positive: options.bloom_false_positive,
            levels_options: options.levels_options,

            compaction_pin_ttl: options.compaction_pin_ttl,
            health_timeout: options.health_timeout,
        }
    }

    fn create_context(&self, level: u64) -> CompactionContext {
        CompactionContext {
            level,
            meta_store: self.meta_store.clone(),
            version_manager: self.version_manager.clone(),
            trigger_l0_compaction_ssts: self.trigger_l0_compaction_ssts,
            sstable_capacity: self.sstable_capacity,
            block_capacity: self.block_capacity,
            restart_interval: self.restart_interval,
            bloom_false_positive: self.bloom_false_positive,
            levels_options: self.levels_options.clone(),
            compaction_pin_ttl: self.compaction_pin_ttl,
            health_timeout: self.health_timeout,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        let mut trigger_l0_ticker = tokio::time::interval(self.trigger_l0_compaction_interval);

        loop {
            tokio::select! {
                _ = trigger_l0_ticker.tick() => {
                    trace!("tick l0 compaction [interval: {:?}]", self.trigger_l0_compaction_interval);
                    let ctx = self.create_context(0);
                    tokio::spawn(
                        async move {
                            if let Err(e) = trigger(ctx).await {
                                error!("trigger compaction l0 error: {}", e);
                                println!("trigger compaction l0 error: {}", e);
                            }
                        }
                    );
                }
            }
        }
    }
}

async fn trigger(ctx: CompactionContext) -> Result<()> {
    let now = SystemTime::now();

    let node_ranges = ctx.meta_store.all_node_ranges().await?;
    let partition_points = node_ranges
        .iter()
        .flat_map(|(_node_id, ranges)| ranges.iter().map(|range| range.start_key.clone()))
        .collect_vec();

    let old_node_ssts = pick_ssts(&ctx, node_ranges, now).await?;

    let old_ssts = old_node_ssts
        .iter()
        .flat_map(|(_node_id, old_ssts)| old_ssts.to_vec())
        .collect_vec();

    if old_ssts.is_empty() {
        return Ok(());
    }
    assert!(verify_no_duplication(old_ssts.iter()));

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
                println!("some sstable has been pinned, skip compaction");
                return Ok(());
            }

            sub_compaction(&sub_ctx, old_ssts.clone(), partition_points_clone, now).await?;

            if let Err(e) = sub_ctx.meta_store.unpin_sstables(&old_ssts.to_vec()).await {
                error!("failed to unpin sstables, will be resolved by timeout");
                println!("failed to unpin sstables, will be resolved by timeout");
                return Err(e);
            }
            Ok(())
        };
        futures.push(future);
    }

    let results = future::join_all(futures).await;
    let errs = results.into_iter().filter(|r| r.is_err()).collect_vec();
    if !errs.is_empty() {
        return Err(Error::Other(format!("compaction error: {:?}", errs)));
    }
    Ok(())
}

// TODO: Pin sstable before `sub_compaction` and unpin after it.
async fn sub_compaction(
    ctx: &CompactionContext,
    old_ssts: TwoLevelSstables,
    partition_points: Vec<PartitionPoint>,
    now: SystemTime,
) -> Result<()> {
    let watermark = ctx.version_manager.watermark().await;

    let req = CompactionRequest {
        sst_ids: old_ssts.to_vec(),
        watermark,
        sstable_capacity: ctx.sstable_capacity as u64,
        block_capacity: ctx.block_capacity as u64,
        restart_interval: ctx.restart_interval as u64,
        bloom_false_positive: ctx.bloom_false_positive,
        compression_algorithm: ctx
            .levels_options
            .get(ctx.level as usize + 1)
            .unwrap_or_else(|| panic!("no config for {}", ctx.level as usize + 1))
            .compression_algorithm
            .into(),
        remove_tombstone: ctx.level as usize + 1 == ctx.levels_options.len() - 1,
        partition_points: partition_points.clone(),
    };

    let exhauster = ctx.meta_store.pick_exhauster(ctx.health_timeout).await?;
    if exhauster.is_none() {
        warn!("no live exhauster, skip compaction");
        return Ok(());
    }
    let exhauster = exhauster.unwrap();

    let mut client =
        ExhausterServiceClient::connect(format!("http://{}:{}", exhauster.host, exhauster.port))
            .await?;
    let rsp = client.compaction(Request::new(req)).await?.into_inner();

    if now.elapsed().unwrap() > ctx.compaction_pin_ttl {
        warn!("compaction timeout, skip version updates");
        return Ok(());
    }

    let new_ssts = rsp.sst_ids;

    let mut sstable_diffs =
        Vec::with_capacity(old_ssts.first.len() + old_ssts.second.len() + new_ssts.len());

    for sst_id in old_ssts.first.iter() {
        sstable_diffs.push(SsTableDiff {
            id: *sst_id,
            level: ctx.level,
            op: SsTableOp::Delete.into(),
        });
    }
    for sst_id in old_ssts.second.iter() {
        sstable_diffs.push(SsTableDiff {
            id: *sst_id,
            level: ctx.level + 1,
            op: SsTableOp::Delete.into(),
        });
    }
    for sst_id in new_ssts.iter() {
        sstable_diffs.push(SsTableDiff {
            id: *sst_id,
            level: ctx.level + 1,
            op: SsTableOp::Insert.into(),
        });
    }

    let version_diff = VersionDiff {
        id: 0,
        sstable_diffs,
    };
    ctx.version_manager.update(version_diff, false).await?;

    Ok(())
}

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
                    &range.start_key..=&range.end_key,
                )
                .await?;
            assert_eq!(base_range_ssts.len(), 1);
            base_level_ssts.extend(base_range_ssts[0].iter());
            // TODO: Stop picking if there is already too many.
        }
        if ctx.level == 0 && base_level_ssts.len() < ctx.trigger_l0_compaction_ssts {
            continue;
        }
        for base_sst in base_level_ssts.iter() {
            let next_ssts = ctx
                .version_manager
                .pick_overlap_ssts_by_sst_id(
                    ctx.level as usize + 1..ctx.level as usize + 2,
                    *base_sst,
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
