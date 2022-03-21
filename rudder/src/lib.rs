pub mod config;
pub mod error;
pub mod meta;
pub mod service;
pub mod worker;

use std::sync::Arc;

use bytesize::ByteSize;
use config::RudderConfig;
use error::{config_err, err, Result};
use meta::mem::MemoryMetaStore;
use meta::MetaStoreRef;
use runkv_common::BoxedWorker;
use runkv_proto::rudder::rudder_service_server::RudderServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Rudder, RudderOptions};
use tonic::transport::Server;
use tracing::info;
use worker::compactor::{Compactor, CompactorOptions};

pub async fn bootstrap_rudder(
    config: &RudderConfig,
    rudder: Rudder,
    workers: Vec<BoxedWorker>,
) -> Result<()> {
    let addr_str = format!("{}:{}", config.host, config.port);

    for mut worker in workers.into_iter() {
        tokio::spawn(async move { worker.run().await });
    }

    Server::builder()
        .add_service(RudderServiceServer::new(rudder))
        .serve(addr_str.parse().map_err(err)?)
        .await
        .map_err(err)
}

pub async fn build_rudder(config: &RudderConfig) -> Result<(Rudder, Vec<BoxedWorker>)> {
    let object_store = build_object_store(config).await;
    build_rudder_with_object_store(config, object_store).await
}

pub async fn build_rudder_with_object_store(
    config: &RudderConfig,
    object_store: ObjectStoreRef,
) -> Result<(Rudder, Vec<BoxedWorker>)> {
    let sstable_store = build_sstable_store(config, object_store)?;

    let version_manager = build_version_manager(config, sstable_store.clone())?;

    let meta_store = build_meta_store();

    let compactor = build_compactor(config, meta_store.clone(), version_manager.clone())?;

    let options = RudderOptions {
        version_manager,
        sstable_store,
        meta_store,
    };

    let rudder = Rudder::new(options);

    Ok((rudder, vec![compactor]))
}

async fn build_object_store(config: &RudderConfig) -> ObjectStoreRef {
    if let Some(c) = &config.s3 {
        info!("s3 config found, create s3 object store");
        Arc::new(S3ObjectStore::new(c.bucket.clone()).await)
    } else if let Some(c) = &config.minio {
        info!("minio config found, create minio object store");
        Arc::new(S3ObjectStore::new_with_minio(&c.url).await)
    } else {
        info!("no object store config found, create default memory object store");
        Arc::new(MemObjectStore::default())
    }
}

fn build_sstable_store(
    config: &RudderConfig,
    object_store: ObjectStoreRef,
) -> Result<SstableStoreRef> {
    let block_cache = BlockCache::new(0);
    let sstable_store_options = SstableStoreOptions {
        path: config.data_path.clone(),
        object_store,
        block_cache,
        meta_cache_capacity: config
            .cache
            .meta_cache_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
    };
    let sstable_store = SstableStore::new(sstable_store_options);
    Ok(Arc::new(sstable_store))
}

fn build_version_manager(
    config: &RudderConfig,
    sstable_store: SstableStoreRef,
) -> Result<VersionManager> {
    let version_manager_options = VersionManagerOptions {
        levels_options: config.lsm_tree.levels_options.clone(),
        // TODO: Recover from meta or scanning.
        levels: vec![vec![]; config.lsm_tree.levels_options.len()],
        sstable_store,
    };
    Ok(VersionManager::new(version_manager_options))
}

fn build_meta_store() -> MetaStoreRef {
    // TODO: Build with storage.
    Arc::new(MemoryMetaStore::default())
}

fn build_compactor(
    config: &RudderConfig,
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
) -> Result<BoxedWorker> {
    let compactor_options = CompactorOptions {
        meta_store,
        version_manager,
        trigger_l0_compaction_ssts: config.lsm_tree.trigger_l0_compaction_ssts,
        trigger_l0_compaction_interval: config
            .lsm_tree
            .trigger_compaction_interval
            .parse::<humantime::Duration>()
            .map_err(config_err)?
            .into(),
        trigger_compaction_interval: config
            .lsm_tree
            .trigger_compaction_interval
            .parse::<humantime::Duration>()
            .map_err(config_err)?
            .into(),
        sstable_capacity: config
            .lsm_tree
            .sstable_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
        block_capacity: config
            .lsm_tree
            .block_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
        restart_interval: config.lsm_tree.restart_interval,
        bloom_false_positive: config.lsm_tree.bloom_false_positive,
        levels_options: config.lsm_tree.levels_options.clone(),
        health_timeout: config
            .health_timeout
            .parse::<humantime::Duration>()
            .map_err(config_err)?
            .into(),
    };

    Ok(Box::new(Compactor::new(compactor_options)))
}
