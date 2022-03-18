pub mod config;
pub mod error;
pub mod meta;
pub mod service;
pub mod store;

use std::fs::read_to_string;
use std::sync::Arc;

use bytesize::ByteSize;
use clap::Parser;
use error::Result;
use runkv_proto::runkv::rudder_service_server::RudderServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::{MemObjectStore, ObjectStore, S3ObjectStore};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

use crate::config::RudderConfig;
use crate::error::{config_err, err};
use crate::service::{Rudder, RudderOptions};

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "etc/rudder.toml")]
    config_file_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).map_err(err)?;

    let args = Args::parse();
    info!("args: {:?}", args);

    let config: RudderConfig =
        toml::from_str(&read_to_string(&args.config_file_path).map_err(err)?)
            .map_err(config_err)?;
    info!("config: {:?}", config);

    let sstable_store = create_sstable_store(&config).await?;

    let version_manager = create_version_manager(&config, sstable_store.clone())?;

    let options = RudderOptions {
        version_manager,
        sstable_store,
    };

    let rudder = Rudder::new(options);

    let addr_str = format!("{}:{}", config.host, config.port);

    Server::builder()
        .add_service(RudderServiceServer::new(rudder))
        .serve(addr_str.parse().map_err(err)?)
        .await
        .map_err(err)
}

async fn create_sstable_store(config: &RudderConfig) -> Result<SstableStoreRef> {
    let object_store: Arc<dyn ObjectStore> = if let Some(c) = &config.s3 {
        info!("s3 config found, create s3 object store");
        Arc::new(S3ObjectStore::new(c.bucket.clone()).await)
    } else if let Some(c) = &config.minio {
        info!("minio config found, create minio object store");
        Arc::new(S3ObjectStore::new_with_minio(&c.url).await)
    } else {
        info!("no object store config found, create default memory object store");
        Arc::new(MemObjectStore::default())
    };
    let block_cache = BlockCache::new(0);
    let sstable_store_options = SstableStoreOptions {
        path: config.data_path.clone(),
        object_store,
        block_cache,
        meta_cache_capacity: config
            .cache
            .meta_capacity
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
