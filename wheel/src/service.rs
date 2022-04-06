use async_trait::async_trait;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::{KvRequest, KvResponse, UpdateKeyRangesRequest, UpdateKeyRangesResponse};
use tonic::{Request, Response, Status};

use crate::meta::MetaStoreRef;
use crate::storage::lsm_tree::ObjectStoreLsmTree;

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct WheelOptions {
    pub lsm_tree: ObjectStoreLsmTree,
    pub meta_store: MetaStoreRef,
}

pub struct Wheel {
    _options: WheelOptions,
    _lsm_tree: ObjectStoreLsmTree,
    meta_store: MetaStoreRef,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            _lsm_tree: options.lsm_tree.clone(),
            meta_store: options.meta_store.clone(),
            _options: options,
        }
    }
}

#[async_trait]
impl WheelService for Wheel {
    async fn update_key_ranges(
        &self,
        request: Request<UpdateKeyRangesRequest>,
    ) -> core::result::Result<Response<UpdateKeyRangesResponse>, Status> {
        let req = request.into_inner();
        self.meta_store
            .update_key_ranges(req.key_ranges)
            .await
            .map_err(internal)?;
        let rsp = UpdateKeyRangesResponse::default();
        Ok(Response::new(rsp))
    }

    async fn kv(&self, _request: Request<KvRequest>) -> Result<Response<KvResponse>, Status> {
        todo!()
    }
}
