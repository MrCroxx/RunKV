use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref RAFT_LOG_STORE_SYNC_COUNTER_VEC: prometheus::CounterVec =
        prometheus::register_counter_vec!(
            "raft_log_store_sync_counter_vec",
            "raft log store sync counter vec",
            &["node"]
        )
        .unwrap();
}

pub struct RaftLogStoreMetrics {
    pub sync_counter: prometheus::Counter,
}

pub type RaftLogStoreMetricsRef = Arc<RaftLogStoreMetrics>;

impl RaftLogStoreMetrics {
    pub fn new(node: u64) -> Self {
        Self {
            sync_counter: RAFT_LOG_STORE_SYNC_COUNTER_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
        }
    }
}
