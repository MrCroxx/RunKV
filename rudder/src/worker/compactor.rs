use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use futures::future;
use itertools::Itertools;
use runkv_common::Worker;
use runkv_proto::exhauster::exhauster_service_client::ExhausterServiceClient;
use runkv_proto::exhauster::CompactionRequest;
use runkv_proto::manifest::{SsTableDiff, SsTableOp, VersionDiff};
use runkv_storage::manifest::{LevelOptions, VersionManager};
use tonic::Request;
use tracing::warn;

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

            health_timeout: options.health_timeout,
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        tokio::time::sleep(self.trigger_l0_compaction_interval).await;
        self.trigger_l0().await
    }

    async fn trigger_l0(&mut self) -> Result<()> {
        let node_ranges = self.meta_store.all_node_ranges().await?;
        let partition_points = node_ranges
            .iter()
            .flat_map(|(_node_id, ranges)| ranges.iter().map(|range| range.start_key.clone()))
            .collect_vec();

        let mut old_node_ssts: BTreeMap<u64, (Vec<u64>, Vec<u64>)> = BTreeMap::default();

        // NOTE: Assume sstables from different range/node will not overlap for now.
        // NOTE: The assumption may be changed after scale out.
        for (node_id, ranges) in node_ranges {
            let mut node_l0_ssts: BTreeSet<u64> = BTreeSet::default();
            let mut node_l1_ssts: BTreeSet<u64> = BTreeSet::default();
            for range in ranges {
                let range_l0_ssts = self
                    .version_manager
                    .pick_overlap_ssts(0..1, &range.start_key..=&range.end_key)
                    .await?;
                assert_eq!(range_l0_ssts.len(), 1);
                node_l0_ssts.extend(range_l0_ssts[0].iter());
            }
            if node_l0_ssts.len() < self.trigger_l0_compaction_ssts {
                continue;
            }
            for l0_sst in node_l0_ssts.iter() {
                let l1_ssts = self
                    .version_manager
                    .pick_overlap_ssts_by_sst_id(1..2, *l0_sst)
                    .await?;
                assert_eq!(l1_ssts.len(), 1);
                node_l1_ssts.extend(l1_ssts[0].iter());
            }
            old_node_ssts.insert(
                node_id,
                (
                    node_l0_ssts.into_iter().collect_vec(),
                    node_l1_ssts.into_iter().collect_vec(),
                ),
            );
        }

        // Assert no duplicated ssts.
        let all_ssts = old_node_ssts
            .iter()
            .flat_map(|(_node_id, (l0_ssts, l1_ssts))| {
                l0_ssts.iter().chain(l1_ssts.iter()).copied()
            })
            .collect_vec();
        assert!(verify_no_duplication(all_ssts.iter()));

        let watermark = self.version_manager.watermark().await;
        let reqs = old_node_ssts
            .iter()
            .map(|(_node_id, (l0_ssts, l1_ssts))| CompactionRequest {
                sst_ids: l0_ssts.iter().chain(l1_ssts.iter()).copied().collect_vec(),
                watermark,
                sstable_capacity: self.sstable_capacity as u64,
                block_capacity: self.block_capacity as u64,
                restart_interval: self.restart_interval as u64,
                bloom_false_positive: self.bloom_false_positive,
                compression_algorithm: self
                    .levels_options
                    .get(0)
                    .expect("no l0 config")
                    .compression_algorithm
                    .into(),
                remove_tombstone: false,
                partition_points: partition_points.clone(),
            })
            .collect_vec();

        // println!("to compact: {:?}", reqs);

        let mut futures = Vec::with_capacity(reqs.len());

        for req in reqs {
            let exhauster = self.meta_store.pick_exhauster(self.health_timeout).await?;
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
                    println!("compaction error: {}", e);
                    warn!("compaction error: {}", e);
                }
            }
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
        self.version_manager.update(version_diff, false).await?;
        Ok(())
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
                    println!("error occur when uploader running: {}", e);
                    warn!("error occur when uploader running: {}", e);
                }
            }
        }
    }
}
