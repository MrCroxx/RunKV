use async_trait::async_trait;
use itertools::Itertools;
use runkv_proto::manifest::{SsTableDiff, SsTableOp, VersionDiff};
use runkv_proto::rudder::rudder_service_server::RudderService;
use runkv_proto::rudder::{HeartbeatRequest, HeartbeatResponse, InsertL0Request, InsertL0Response};
use runkv_storage::components::SstableStoreRef;
use runkv_storage::manifest::VersionManager;
use tonic::{Request, Response, Status};

const DEFAULT_VERSION_DIFF_BATCH: usize = 10;

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct RudderOptions {
    pub version_manager: VersionManager,
    pub sstable_store: SstableStoreRef,
}

pub struct Rudder {
    /// Manifest of sstables.
    version_manager: VersionManager,
    /// The smallest pinned timestamp. Any data whose timestamp is smaller than `watermark` can be
    /// safely delete.
    ///
    /// `wheel node` maintains its own watermark, and `rudder node` collects watermarks from each
    /// `wheel node` periodically and choose the min watermark among them as its own watermark.
    _watermark: u64,
    _sstable_store: SstableStoreRef,
}

impl Rudder {
    pub fn new(options: RudderOptions) -> Self {
        Self {
            version_manager: options.version_manager,
            // TODO: Restore from meta store.
            _watermark: 0,
            _sstable_store: options.sstable_store,
        }
    }
}

#[async_trait]
impl RudderService for Rudder {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> core::result::Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let version_diffs = self
            .version_manager
            .version_diffs_from(req.next_version_id, DEFAULT_VERSION_DIFF_BATCH)
            .await
            .map_err(internal)?;
        let rsp = HeartbeatResponse { version_diffs };
        Ok(Response::new(rsp))
    }

    async fn insert_l0(
        &self,
        request: Request<InsertL0Request>,
    ) -> core::result::Result<Response<InsertL0Response>, Status> {
        let req = request.into_inner();
        let diff = VersionDiff {
            id: 0,
            sstable_diffs: req
                .sst_ids
                .iter()
                .map(|&sst_id| SsTableDiff {
                    id: sst_id,
                    level: 0,
                    op: SsTableOp::Insert.into(),
                })
                .collect_vec(),
        };
        self.version_manager
            .update(diff, false)
            .await
            .map_err(internal)?;
        let version_diffs = self
            .version_manager
            .version_diffs_from(req.next_version_id, usize::MAX)
            .await
            .map_err(internal)?;
        let rsp = InsertL0Response { version_diffs };
        Ok(Response::new(rsp))
    }
}
