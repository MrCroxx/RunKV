use async_trait::async_trait;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_storage::object_store_lsm_tree::ObjectStoreLsmTree;
use runkv_storage::sstable_uploader::SstableUploaderRef;
// use runkv_storage::LsmTree;

pub struct Wheel {
    _options: WheelOptions,
    _lsm_tree: ObjectStoreLsmTree,
    _sstable_uploader: SstableUploaderRef,
}

pub struct WheelOptions {
    pub lsm_tree: ObjectStoreLsmTree,
    pub sstable_uploader: SstableUploaderRef,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            _lsm_tree: options.lsm_tree.clone(),
            _sstable_uploader: options.sstable_uploader.clone(),
            _options: options,
        }
    }
}

#[async_trait]
impl WheelService for Wheel {}
