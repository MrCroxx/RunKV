pub mod compaction_filter;
pub mod config;
pub mod error;
pub mod service;

use std::sync::Arc;

use bytesize::ByteSize;
use config::ExhausterConfig;
use error::{config_err, err, Result};
use runkv_proto::exhauster::exhauster_service_server::ExhausterServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Exhauster, ExhausterOptions};
use tonic::transport::Server;
use tracing::info;

pub async fn bootstrap_exhauster(config: &ExhausterConfig, exhauster: Exhauster) -> Result<()> {
    let addr_str = format!("{}:{}", config.host, config.port);

    Server::builder()
        .add_service(ExhausterServiceServer::new(exhauster))
        .serve(addr_str.parse().map_err(config_err)?)
        .await
        .map_err(err)
}

pub async fn build_exhauster(config: &ExhausterConfig) -> Result<Exhauster> {
    let object_store = create_object_store(config).await;
    build_exhauster_with_object_store(config, object_store).await
}

pub async fn build_exhauster_with_object_store(
    config: &ExhausterConfig,
    object_store: ObjectStoreRef,
) -> Result<Exhauster> {
    let sstable_store = create_sstable_store(config, object_store)?;

    let options = ExhausterOptions {
        node_id: config.id,
        sstable_store,
        // TODO: Restore from persistent store.
        sstable_sequential_id: 1,
    };

    let exhauster = Exhauster::new(options);

    Ok(exhauster)
}

async fn create_object_store(config: &ExhausterConfig) -> ObjectStoreRef {
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
    config: &ExhausterConfig,
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
