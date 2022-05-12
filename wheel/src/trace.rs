use lazy_static::lazy_static;
use runkv_common::sharded_hash_map::ShardedHashMap;

lazy_static! {
    pub static ref TRACE_CTX: TraceContext = TraceContext::default();
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct RequestContext {
    pub id: u64,
    pub raft_node: u64,
}

#[derive(Default)]
pub struct TraceContext {
    pub propose_ts: ShardedHashMap<RequestContext, u64>,
    pub raft_propose_ts: ShardedHashMap<RequestContext, u64>,
    pub raft_send_ts: ShardedHashMap<RequestContext, u64>,
    pub raft_append_ts: ShardedHashMap<RequestContext, u64>,
    pub raft_apply_ts: ShardedHashMap<RequestContext, u64>,
}
