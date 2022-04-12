use serde::{Deserialize, Serialize};

pub mod fsm;
pub mod lsm_tree;
pub mod raft_log_store;

pub type RaftNodeId = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RaftRequest {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RaftResponse {}

openraft::declare_raft_types!(
    pub RaftTypeConfig: D = RaftRequest, R = RaftResponse, NodeId = RaftNodeId
);
