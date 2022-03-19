pub mod config;
pub mod error;
pub mod service;
pub mod store;

use std::sync::Arc;

use bytesize::ByteSize;
use config::RudderConfig;
use error::{config_err, err, Result};
use runkv_proto::rudder::rudder_service_server::RudderServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Rudder, RudderOptions};
use tonic::transport::Server;
use tracing::info;

pub async fn bootstrap_rudder(config: &RudderConfig, rudder: Rudder) -> Result<()> {
    let addr_str = format!("{}:{}", config.host, config.port);

    Server::builder()
        .add_service(RudderServiceServer::new(rudder))
        .serve(addr_str.parse().map_err(err)?)
        .await
        .map_err(err)
}

pub async fn build_rudder(config: &RudderConfig) -> Result<Rudder> {
    let object_store = create_object_store(config).await;
    build_rudder_with_object_store(config, object_store).await
}

pub async fn build_rudder_with_object_store(
    config: &RudderConfig,
    object_store: ObjectStoreRef,
) -> Result<Rudder> {
    let sstable_store = create_sstable_store(config, object_store)?;

    let version_manager = create_version_manager(config, sstable_store.clone())?;

    let options = RudderOptions {
        version_manager,
        sstable_store,
    };

    let rudder = Rudder::new(options);

    Ok(rudder)
}

async fn create_object_store(config: &RudderConfig) -> ObjectStoreRef {
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

fn create_sstable_store(
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

fn create_version_manager(
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
