pub mod components;
pub mod config;
pub mod error;
pub mod meta;
pub mod service;
pub mod worker;

use std::sync::Arc;

use bytesize::ByteSize;
use components::gear::Gear;
use components::network::RaftNetwork;
use components::raft_manager::{RaftManager, RaftManagerOptions};
use error::{Error, Result};
use meta::mem::MemoryMetaStore;
use meta::MetaStoreRef;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::BoxedWorker;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::wheel::wheel_service_server::WheelServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
use runkv_storage::raft_log_store::RaftLogStore;
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Wheel, WheelOptions};
use tokio::sync::mpsc;
use tonic::transport::Server;
use tracing::info;
use worker::heartbeater::{Heartbeater, HeartbeaterOptions};
use worker::sstable_uploader::{SstableUploader, SstableUploaderOptions};

use crate::components::lsm_tree::{ObjectStoreLsmTree, ObjectStoreLsmTreeOptions};
use crate::config::WheelConfig;

pub async fn bootstrap_wheel(
    config: &WheelConfig,
    wheel: Wheel,
    workers: Vec<BoxedWorker>,
) -> Result<()> {
    let addr_str = format!("{}:{}", config.host, config.port);

    for mut worker in workers.into_iter() {
        tokio::spawn(async move { worker.run().await });
    }

    Server::builder()
        .add_service(WheelServiceServer::new(wheel))
        .serve(addr_str.parse().map_err(Error::err)?)
        .await
        .map_err(Error::err)
}

pub async fn build_wheel(
    config: &WheelConfig,
) -> Result<(Wheel, ObjectStoreLsmTree, Vec<BoxedWorker>)> {
    let object_store = build_object_store(config).await;
    build_wheel_with_object_store(config, object_store).await
}

pub async fn build_wheel_with_object_store(
    config: &WheelConfig,
    object_store: ObjectStoreRef,
) -> Result<(Wheel, ObjectStoreLsmTree, Vec<BoxedWorker>)> {
    let sstable_store = build_sstable_store(config, object_store)?;

    let version_manager = build_version_manager(config, sstable_store.clone())?;

    let lsm_tree = build_lsm_tree(config, sstable_store.clone(), version_manager.clone())?;

    let channel_pool = build_channel_pool(config);

    let sstable_uploader = build_sstable_uploader(
        config,
        lsm_tree.clone(),
        sstable_store,
        version_manager.clone(),
        channel_pool.clone(),
    )?;

    let meta_store = build_meta_store()?;

    let version_syncer = build_heartbeater(
        config,
        version_manager,
        meta_store.clone(),
        channel_pool.clone(),
    )?;

    let (tx, rx) = mpsc::unbounded_channel();
    let gear = Gear::new(tx);

    let raft_log_store = build_raft_log_store(config).await?;
    let raft_network = build_raft_network(channel_pool.clone());
    let raft_manager = build_raft_manager(
        config,
        raft_log_store.clone(),
        raft_network.clone(),
        gear.clone(),
    );

    let options = WheelOptions {
        lsm_tree: lsm_tree.clone(),
        meta_store,
        channel_pool,
        raft_log_store,
        raft_network,
        raft_manager,
        gear_receiver: rx,
    };

    let wheel = Wheel::new(options);

    Ok((
        wheel,
        lsm_tree,
        vec![Box::new(sstable_uploader), Box::new(version_syncer)],
    ))
}

async fn build_object_store(config: &WheelConfig) -> ObjectStoreRef {
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
    config: &WheelConfig,
    object_store: ObjectStoreRef,
) -> Result<SstableStoreRef> {
    let block_cache = BlockCache::new(
        config
            .cache
            .block_cache_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
    );
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

fn build_version_manager(
    config: &WheelConfig,
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

fn build_lsm_tree(
    config: &WheelConfig,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
) -> Result<ObjectStoreLsmTree> {
    let lsm_tree_options = ObjectStoreLsmTreeOptions {
        sstable_store,
        write_buffer_capacity: config
            .buffer
            .write_buffer_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
        version_manager,
    };
    Ok(ObjectStoreLsmTree::new(lsm_tree_options))
}

fn build_sstable_uploader(
    config: &WheelConfig,
    lsm_tree: ObjectStoreLsmTree,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
    channel_pool: ChannelPool,
) -> Result<SstableUploader> {
    let sstable_uploader = SstableUploaderOptions {
        node_id: config.id,
        lsm_tree,
        sstable_store,
        version_manager,
        sstable_capacity: config
            .lsm_tree
            .sstable_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
        block_capacity: config
            .lsm_tree
            .block_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
        restart_interval: config.lsm_tree.restart_interval,
        bloom_false_positive: config.lsm_tree.bloom_false_positive,
        compression_algorithm: config
            .lsm_tree
            .levels_options
            .get(0)
            .ok_or_else(|| Error::Other("no L0 in lsm_tree.levels_options".to_string()))?
            .compression_algorithm,
        poll_interval: config
            .poll_interval
            .parse::<humantime::Duration>()
            .map_err(Error::config_err)?
            .into(),
        channel_pool,
        rudder_node_id: config.rudder.id,
    };
    Ok(SstableUploader::new(sstable_uploader))
}

fn build_heartbeater(
    config: &WheelConfig,
    version_manager: VersionManager,
    meta_store: MetaStoreRef,
    channel_pool: ChannelPool,
) -> Result<Heartbeater> {
    let wheel_version_manager_options = HeartbeaterOptions {
        node_id: config.id,
        version_manager,
        meta_store,
        channel_pool,
        rudder_node_id: config.rudder.id,
        heartbeat_interval: config
            .heartbeat_interval
            .parse::<humantime::Duration>()
            .map_err(Error::config_err)?
            .into(),
        endpoint: PbEndpoint {
            host: config.host.clone(),
            port: config.port as u32,
        },
    };
    Ok(Heartbeater::new(wheel_version_manager_options))
}

fn build_meta_store() -> Result<MetaStoreRef> {
    let meta_store = MemoryMetaStore::default();
    Ok(Arc::new(meta_store))
}

fn build_channel_pool(config: &WheelConfig) -> ChannelPool {
    ChannelPool::with_nodes(vec![config.rudder.clone()])
}

async fn build_raft_log_store(config: &WheelConfig) -> Result<RaftLogStore> {
    let raft_log_store_options = RaftLogStoreOptions {
        log_dir_path: config.raft_log_store.log_dir_path.clone(),
        log_file_capacity: config
            .raft_log_store
            .log_file_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
        block_cache_capacity: config
            .raft_log_store
            .block_cache_capacity
            .parse::<ByteSize>()
            .map_err(Error::config_err)?
            .0 as usize,
    };
    RaftLogStore::open(raft_log_store_options)
        .await
        .map_err(Error::storage_err)
}

fn build_raft_network(channel_pool: ChannelPool) -> RaftNetwork {
    RaftNetwork::new(channel_pool)
}

fn build_raft_manager(
    config: &WheelConfig,
    raft_log_store: RaftLogStore,
    raft_network: RaftNetwork,
    gear: Gear,
) -> RaftManager {
    let raft_manager_options = RaftManagerOptions {
        node: config.id,
        raft_log_store,
        raft_network,
        gear,
    };
    RaftManager::new(raft_manager_options)
}
