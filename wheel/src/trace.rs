use lazy_static::lazy_static;
use runkv_common::sharded_hash_map::ShardedHashMap;

lazy_static! {
    pub static ref TRACE_CTX: TraceContext = TraceContext::default();
    pub static ref TRACE_RAFT_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "trace_raft_latency_histogram_vec",
            "trace raft latency histogram vec",
            &["op", "node", "group", "raft_node"],
            vec![0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0]
        )
        .unwrap();
}

#[derive(Default)]
pub struct TraceContext {
    pub propose_ts: ShardedHashMap<u64, u64>,
}
