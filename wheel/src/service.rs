use async_trait::async_trait;
use runkv_proto::wheel::wheel_service_server::WheelService;

use crate::storage::lsm_tree::ObjectStoreLsmTree;

pub struct WheelOptions {
    pub lsm_tree: ObjectStoreLsmTree,
}

pub struct Wheel {
    _options: WheelOptions,
    _lsm_tree: ObjectStoreLsmTree,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            _lsm_tree: options.lsm_tree.clone(),
            _options: options,
        }
    }
}

#[async_trait]
impl WheelService for Wheel {}
