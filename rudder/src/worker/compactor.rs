use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::future;
use itertools::Itertools;
use runkv_common::Worker;
use runkv_proto::exhauster::exhauster_service_client::ExhausterServiceClient;
use runkv_proto::exhauster::CompactionRequest;
use runkv_proto::manifest::{SsTableDiff, SsTableOp, VersionDiff};
use runkv_storage::manifest::{LevelOptions, VersionManager};
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

    async fn run_inner(&mut self) -> Result<()> {
        let mut trigger_l0_ticker = tokio::time::interval(self.trigger_l0_compaction_interval);

        loop {
            let meta_store = self.meta_store.clone();
            let version_manager = self.version_manager.clone();
            let trigger_l0_compaction_ssts = self.trigger_l0_compaction_ssts;
            let sstable_capacity = self.sstable_capacity;
            let block_capacity = self.block_capacity;
            let restart_interval = self.restart_interval;
            let bloom_false_positive = self.bloom_false_positive;
            let levels_options = self.levels_options.clone();
            let compaction_pin_ttl = self.compaction_pin_ttl;
            let health_timeout = self.health_timeout;

            tokio::select! {
                _ = trigger_l0_ticker.tick() => {
                    trace!("tick l0 compaction [interval: {:?}]", self.trigger_l0_compaction_interval);
                    tokio::spawn(
                        async move {
                            if let Err(e) = Self::trigger_l0(
                                meta_store.clone(),
                                version_manager.clone(),
                                trigger_l0_compaction_ssts,
                                sstable_capacity,
                                block_capacity,
                                restart_interval,
                                bloom_false_positive,
                                levels_options.clone(),
                                compaction_pin_ttl,
                                health_timeout,
                            ).await {
                                error!("trigger compaction l0 error: {}", e);
                                println!("trigger compaction l0 error: {}", e);
                            }
                        }
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn trigger_l0(
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
    ) -> Result<()> {
        let node_ranges = meta_store.all_node_ranges().await?;
        let partition_points = node_ranges
            .iter()
            .flat_map(|(_node_id, ranges)| ranges.iter().map(|range| range.start_key.clone()))
            .collect_vec();

        let mut old_node_ssts: BTreeMap<u64, (Vec<u64>, Vec<u64>)> = BTreeMap::default();

        let now = SystemTime::now();

        // NOTE: Assume sstables from different range/node will not overlap for now.
        // NOTE: The assumption may be changed after scale out.
        for (node_id, ranges) in node_ranges {
            let mut node_l0_ssts: BTreeSet<u64> = BTreeSet::default();
            let mut node_l1_ssts: BTreeSet<u64> = BTreeSet::default();
            for range in ranges {
                let range_l0_ssts = version_manager
                    .pick_overlap_ssts(0..1, &range.start_key..=&range.end_key)
                    .await?;
                assert_eq!(range_l0_ssts.len(), 1);
                node_l0_ssts.extend(range_l0_ssts[0].iter());
            }
            if node_l0_ssts.len() < trigger_l0_compaction_ssts {
                continue;
            }
            for l0_sst in node_l0_ssts.iter() {
                let l1_ssts = version_manager
                    .pick_overlap_ssts_by_sst_id(1..2, *l0_sst)
                    .await?;
                assert_eq!(l1_ssts.len(), 1);
                node_l1_ssts.extend(l1_ssts[0].iter());
            }
            let node_l0_ssts = node_l0_ssts.into_iter().collect_vec();
            let node_l1_ssts = node_l1_ssts.into_iter().collect_vec();

            if meta_store
                .is_sstables_pinned(&node_l0_ssts, now)
                .await?
                .iter()
                .chain(
                    meta_store
                        .is_sstables_pinned(&node_l1_ssts, now)
                        .await?
                        .iter(),
                )
                .any(|pinned| *pinned)
            {
                continue;
            }

            old_node_ssts.insert(node_id, (node_l0_ssts, node_l1_ssts));
        }

        let old_ssts = old_node_ssts
            .iter()
            .flat_map(|(_node_id, (l0_ssts, l1_ssts))| {
                l0_ssts.iter().chain(l1_ssts.iter()).copied()
            })
            .collect_vec();

        if old_ssts.is_empty() {
            return Ok(());
        }
        assert!(verify_no_duplication(old_ssts.iter()));

        if !meta_store.pin_sstables(&old_ssts, now).await? {
            warn!("some sstables to comapct are pinned, retry in next loop");
            println!("some sstables to comapct are pinned, retry in next loop");
            return Ok(());
        }

        let watermark = version_manager.watermark().await;
        let reqs = old_node_ssts
            .iter()
            .map(|(_node_id, (l0_ssts, l1_ssts))| CompactionRequest {
                sst_ids: l0_ssts.iter().chain(l1_ssts.iter()).copied().collect_vec(),
                watermark,
                sstable_capacity: sstable_capacity as u64,
                block_capacity: block_capacity as u64,
                restart_interval: restart_interval as u64,
                bloom_false_positive,
                compression_algorithm: levels_options
                    .get(0)
                    .expect("no l0 config")
                    .compression_algorithm
                    .into(),
                remove_tombstone: false,
                partition_points: partition_points.clone(),
            })
            .collect_vec();

        let mut futures = Vec::with_capacity(reqs.len());

        for req in reqs {
            let exhauster = Self::unpin_on_err(
                meta_store.clone(),
                meta_store.pick_exhauster(health_timeout).await,
                &old_ssts,
            )
            .await?;
            let future = async move {
                if let Some(endpoint) = exhauster {
                    let mut client = ExhausterServiceClient::connect(format!(
                        "http://{}:{}",
                        endpoint.host, endpoint.port
                    ))
                    .await?;
                    let rsp = client.compaction(Request::new(req)).await?.into_inner();
                    Ok(rsp.sst_ids)
                } else {
                    Err(Error::Other("no valid exhuaster".to_string()))
                }
            };
            futures.push(future);
        }
        let results = future::join_all(futures).await;

        let mut new_l1_ssts = Vec::with_capacity(100);
        for result in results {
            match result {
                Ok(mut sst_ids) => new_l1_ssts.append(&mut sst_ids),
                Err(e) => {
                    // println!("compaction error: {}", e);
                    warn!("compaction error: {}", e);
                }
            }
        }

        // FIX ME: Consistency broken when time drifts. Check ttl in version manager?
        if now.elapsed().unwrap() > compaction_pin_ttl {
            println!("compaction time out, skip updating version");
            warn!("compaction time out, skip updating version");
            return Err(Error::Other("compaction timeout".to_string()));
        }

        let sstable_diffs = old_node_ssts
            .into_iter()
            .flat_map(|(_node_id, (l0_ssts, l1_ssts))| {
                l0_ssts
                    .into_iter()
                    .map(|sst_id| SsTableDiff {
                        id: sst_id,
                        level: 0,
                        op: SsTableOp::Delete.into(),
                    })
                    .chain(l1_ssts.into_iter().map(|sst_id| SsTableDiff {
                        id: sst_id,
                        level: 1,
                        op: SsTableOp::Delete.into(),
                    }))
            })
            .chain(new_l1_ssts.into_iter().map(|sst_id| SsTableDiff {
                id: sst_id,
                level: 1,
                op: SsTableOp::Insert.into(),
            }))
            .collect_vec();

        let version_diff = VersionDiff {
            id: 0,
            sstable_diffs,
        };
        Self::unpin_on_err(
            meta_store.clone(),
            version_manager
                .update(version_diff, false)
                .await
                .map_err(Error::StorageError),
            &old_ssts,
        )
        .await?;
        if let Err(ue) = meta_store.unpin_sstables(&old_ssts).await {
            error!("fail to unpin sstables [sstables: {:?}] : {}", old_ssts, ue);
        }
        Ok(())
    }

    async fn unpin_on_err<T>(
        meta_store: MetaStoreRef,
        result: Result<T>,
        sst_ids: &[u64],
    ) -> Result<T> {
        if result.is_err() {
            if let Err(e) = meta_store.unpin_sstables(sst_ids).await {
                error!("fail to unpin sstables [sstables: {:?}] : {}", sst_ids, e);
            }
        }
        result
    }
}

fn verify_no_duplication(mut iter: core::slice::Iter<u64>) -> bool {
    let mut unique = HashSet::new();
    iter.all(|v| unique.insert(*v))
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
