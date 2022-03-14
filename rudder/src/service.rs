use async_trait::async_trait;
use runkv_proto::runkv::rudder_service_server::RudderService;
use runkv_proto::runkv::{HeartbeatRequest, HeartbeatResponse};
use tonic::{Request, Response, Status};

use crate::MetaManagerRef;

pub struct Rudder {
    _version_manager: MetaManagerRef,
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
