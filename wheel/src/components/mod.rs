pub mod command;
pub mod fsm;
pub mod gear;
pub mod lsm_tree;
pub mod network;
pub mod raft_log_store;
pub mod raft_manager;

use runkv_proto::wheel::{KvRequest, KvResponse};

use self::gear::Gear;
use self::network::RaftNetwork;
use self::raft_log_store::RaftGroupLogStore;

pub type RaftNodeId = u64;

openraft::declare_raft_types!(
    pub RaftTypeConfig: D = KvRequest, R = Option<KvResponse>, NodeId = RaftNodeId
);

pub type Raft = openraft::Raft<RaftTypeConfig, RaftNetwork, RaftGroupLogStore<Gear>>;
