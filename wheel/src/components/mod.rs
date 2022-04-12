pub mod fsm;
pub mod lsm_tree;
pub mod network;
pub mod raft_log_store;

use runkv_proto::wheel::{KvRequest, KvResponse};

pub type RaftNodeId = u64;

openraft::declare_raft_types!(
    pub RaftTypeConfig: D = KvRequest, R = Option<KvResponse>, NodeId = RaftNodeId
);
