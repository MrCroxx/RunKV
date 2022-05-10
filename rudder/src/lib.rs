pub mod config;
pub mod error;
pub mod meta;
pub mod service;
pub mod worker;

use std::sync::Arc;

use bytesize::ByteSize;
use config::RudderConfig;
use error::{Error, Result};
use meta::mem::MemoryMetaStore;
use meta::MetaStoreRef;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::BoxedWorker;
use runkv_proto::rudder::rudder_service_server::RudderServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Rudder, RudderOptions};
use tonic::transport::Server;
use tracing::info;
use worker::compaction_detector::{CompactionDetector, CompactionDetectorOptions};

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
        .serve(addr_str.parse().map_err(Error::err)?)
        .await
        .map_err(Error::err)
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

    let version_manager = build_version_manager(config, sstable_store.clone()).await?;

    let meta_store = build_meta_store(config)?;

    let channel_pool = ChannelPool::default();

    let compaction_detector = build_compaction_detector(
        config,
        meta_store.clone(),
        version_manager.clone(),
        channel_pool.clone(),
    )?;

    let options = RudderOptions {
        version_manager,
        sstable_store,
        meta_store,
        channel_pool,
    };

    let rudder = Rudder::new(options);

    Ok((rudder, vec![compaction_detector]))
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
    let block_cache = BlockCache::new(0, config.id);
    let sstable_store_options = SstableStoreOptions {
        path: config.data_path.clone(),
        object_store,
        block_cache,
        meta_cache_capacity: config
            .cache
            .meta_cache_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
    };
    let sstable_store = SstableStore::new(sstable_store_options);
    Ok(Arc::new(sstable_store))
}

async fn build_version_manager(
    config: &RudderConfig,
    sstable_store: SstableStoreRef,
) -> Result<VersionManager> {
    let version_manager_options = VersionManagerOptions {
        levels_options: config.lsm_tree.levels_options.clone(),
        // TODO: Recover from meta or scanning.
        levels: vec![vec![]; config.lsm_tree.levels_options.len()],
        sstable_store,
    };
    let version_manager = VersionManager::new(version_manager_options);
    version_manager
        .update(
            runkv_proto::manifest::VersionDiff {
                id: 0,
                sstable_diffs: vec![],
            },
            false,
        )
        .await?;
    Ok(version_manager)
}

fn build_meta_store(config: &RudderConfig) -> Result<MetaStoreRef> {
    // TODO: Build with storage.
    let meta_store = MemoryMetaStore::new(
        config
            .lsm_tree
            .compaction_pin_ttl
            .parse::<humantime::Duration>()
            .map_err(Error::config_err)?
            .into(),
    );
    Ok(Arc::new(meta_store))
}

fn build_compaction_detector(
    config: &RudderConfig,
    meta_store: MetaStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,
) -> Result<BoxedWorker> {
    let compactor_options = CompactionDetectorOptions {
        meta_store,
        version_manager,
        channel_pool,
        lsm_tree_config: config.lsm_tree.clone().try_into()?,
        health_timeout: config
            .health_timeout
            .parse::<humantime::Duration>()
            .map_err(Error::config_err)?
            .into(),
    };

    Ok(Box::new(CompactionDetector::new(compactor_options)))
}
