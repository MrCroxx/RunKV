#![feature(let_chains)]

pub mod components;
pub mod config;
pub mod error;
pub mod meta;
pub mod service;
pub mod worker;

use std::sync::Arc;

use bytesize::ByteSize;
use components::raft_manager::{RaftManager, RaftManagerOptions};
use components::raft_network::GrpcRaftNetwork;
use error::{Error, Result};
use hyper::service::{make_service_fn, service_fn};
use meta::mem::MemoryMetaStore;
use meta::MetaStoreRef;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::notify_pool::NotifyPool;
use runkv_common::BoxedWorker;
use runkv_proto::common::Endpoint as PbEndpoint;
use runkv_proto::kv::kv_service_server::KvServiceServer;
use runkv_proto::kv::TxnResponse;
use runkv_proto::wheel::raft_service_server::RaftServiceServer;
use runkv_proto::wheel::wheel_service_server::WheelServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
use runkv_storage::raft_log_store::RaftLogStore;
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Wheel, WheelOptions};
use tonic::transport::Server;
use tracing::info;
use worker::heartbeater::{Heartbeater, HeartbeaterOptions};

use crate::config::WheelConfig;

pub async fn bootstrap_wheel(
    config: &WheelConfig,
    wheel: Wheel,
    workers: Vec<BoxedWorker>,
) -> Result<()> {
    let enable_metrics = match std::env::var("RUNKV_METRICS") {
        Err(_) => false,
        Ok(val) => val.parse().unwrap(),
    };

    let addr_str = format!("{}:{}", config.host, config.port);

    for mut worker in workers.into_iter() {
        tokio::spawn(async move { worker.run().await });
    }

    if enable_metrics {
        bootstrap_prometheus_service(config);
    }

    Server::builder()
        .add_service(WheelServiceServer::new(wheel.clone()))
        .add_service(RaftServiceServer::new(wheel.clone()))
        .add_service(KvServiceServer::new(wheel))
        .serve(addr_str.parse().map_err(Error::err)?)
        .await
        .map_err(Error::err)
}

pub fn bootstrap_prometheus_service(config: &WheelConfig) {
    let listen_addr = format!("{}:{}", config.prometheus.host, config.prometheus.port);
    let prometheus_service_addr = listen_addr.parse().unwrap();
    tokio::spawn(async move {
        info!(
            "Prometheus listener for Prometheus is set up on http://{}",
            listen_addr
        );
        if let Err(e) = hyper::Server::bind(&prometheus_service_addr)
            .serve(make_service_fn(|_| async {
                Ok::<_, hyper::Error>(service_fn(Wheel::prometheus_service))
            }))
            .await
        {
            tracing::error!("promethrus service error: {}", e);
        }
    });
}

pub async fn build_wheel(config: &WheelConfig) -> Result<(Wheel, Vec<BoxedWorker>)> {
    let object_store = build_object_store(config).await;
    build_wheel_with_object_store(config, object_store).await
}

pub async fn build_wheel_with_object_store(
    config: &WheelConfig,
    object_store: ObjectStoreRef,
) -> Result<(Wheel, Vec<BoxedWorker>)> {
    let sstable_store = build_sstable_store(config, object_store)?;

    let version_manager = build_version_manager(config, sstable_store.clone())?;

    let channel_pool = build_channel_pool(config);

    let meta_store = build_meta_store()?;

    let heartbeater = build_heartbeater(
        config,
        version_manager.clone(),
        meta_store.clone(),
        channel_pool.clone(),
    )?;

    let txn_notify_pool = build_txn_notify_pool();

    let raft_log_store = build_raft_log_store(config).await?;
    let raft_network = build_raft_network(config, channel_pool.clone());
    let raft_manager = build_raft_manager(
        config,
        raft_log_store,
        raft_network.clone(),
        txn_notify_pool.clone(),
        version_manager.clone(),
        sstable_store.clone(),
        channel_pool.clone(),
    )?;

    let options = WheelOptions {
        node: config.id,
        meta_store,
        channel_pool,
        raft_network,
        raft_manager,
        txn_notify_pool,
    };

    let wheel = Wheel::new(options);

    Ok((wheel, vec![Box::new(heartbeater)]))
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
        .map_err(Error::StorageError)
}

fn build_raft_network(config: &WheelConfig, channel_pool: ChannelPool) -> GrpcRaftNetwork {
    GrpcRaftNetwork::new(config.id, channel_pool)
}

fn build_raft_manager(
    config: &WheelConfig,
    raft_log_store: RaftLogStore,
    raft_network: GrpcRaftNetwork,
    txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    version_manager: VersionManager,
    sstable_store: SstableStoreRef,
    channel_pool: ChannelPool,
) -> Result<RaftManager> {
    let raft_manager_options = RaftManagerOptions {
        node: config.id,
        rudder_node_id: config.rudder.id,
        raft_log_store,
        raft_network,
        txn_notify_pool,
        version_manager,
        sstable_store,
        channel_pool,
        lsm_tree_options: crate::components::raft_manager::LsmTreeOptions {
            write_buffer_capacity: config
                .buffer
                .write_buffer_capacity
                .parse::<ByteSize>()
                .map_err(Error::config_err)?
                .0 as usize,
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
        },
    };
    Ok(RaftManager::new(raft_manager_options))
}

fn build_txn_notify_pool() -> NotifyPool<u64, Result<TxnResponse>> {
    NotifyPool::default()
}
