use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref RAFT_LOG_STORE_SYNC_DURATION_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "raft_log_store_sync_duration_histogram_vec",
            "raft log store sync duration histogram vec",
            &["node"]
        )
        .unwrap();
    static ref RAFT_LOG_STORE_SYNC_BYTES_GAUGE_VEC: prometheus::GaugeVec =
        prometheus::register_gauge_vec!(
            "raft_log_store_sync_bytes_gauge_vec",
            "raft log store sync bytes guage vec",
            &["node"]
        )
        .unwrap();
}

pub struct RaftLogStoreMetrics {
    pub sync_duration_histogram: prometheus::Histogram,
    pub sync_bytes_guage: prometheus::Gauge,
}

pub type RaftLogStoreMetricsRef = Arc<RaftLogStoreMetrics>;

impl RaftLogStoreMetrics {
    pub fn new(node: u64) -> Self {
        Self {
            sync_duration_histogram: RAFT_LOG_STORE_SYNC_DURATION_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
            sync_bytes_guage: RAFT_LOG_STORE_SYNC_BYTES_GAUGE_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
        }
    }
}
