use std::sync::Arc;

use async_trait::async_trait;
use runkv_proto::runkv::rudder_service_server::RudderService;
use runkv_proto::runkv::{HeartbeatRequest, HeartbeatResponse};
use runkv_storage::components::SstableStoreRef;
use runkv_storage::manifest::VersionManager;
use tonic::{Request, Response, Status};

use crate::meta::{MetaManager, MetaManagerOptions, MetaManagerRef};

pub struct RudderOptions {
    pub version_manager: VersionManager,
    pub sstable_store: SstableStoreRef,
}

pub struct Rudder {
    _meta_manager: MetaManagerRef,
}

impl Rudder {
    pub fn new(options: RudderOptions) -> Self {
        let meta_manager_options = MetaManagerOptions {
            version_manager: options.version_manager.clone(),
            // TODO: Recover it.
            watermark: 0,
            sstable_store: options.sstable_store,
        };
        let meta_manager = Arc::new(MetaManager::new(meta_manager_options));
        Self {
            _meta_manager: meta_manager,
        }
    }
}

#[async_trait]
impl RudderService for Rudder {
    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> core::result::Result<Response<HeartbeatResponse>, Status> {
        todo!()
    }
}
