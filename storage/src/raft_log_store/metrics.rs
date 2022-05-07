use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "raft_log_store_latency_histogram_vec",
            "raft log store latency histogram vec",
            &["op", "node"]
        )
        .unwrap();
    static ref RAFT_LOG_STORE_THROUGHPUT_GAUGE_VEC: prometheus::GaugeVec =
        prometheus::register_gauge_vec!(
            "raft_log_store_throughput_gauge_vec",
            "raft log store throughput guage vec",
            &["op", "node"]
        )
        .unwrap();
    static ref RAFT_LOG_STORE_OP_COUNTER_VEC: prometheus::CounterVec =
        prometheus::register_counter_vec!(
            "raft_log_store_op_counter_vec",
            "raft log store op counter vec",
            &["op", "node"]
        )
        .unwrap();
    static ref RAFT_LOG_STORE_BATCH_WRITERS_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "raft_log_store_batch_writers_histogram_vec",
            "raft log store batch writers histogram vec",
            &["node"]
        )
        .unwrap();
    static ref RAFT_LOG_STORE_SYNC_SIZE_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "raft_log_store_sync_size_histogram_vec",
            "raft log store sync size histogram vec",
            &["node"]
        )
        .unwrap();
}

pub struct RaftLogStoreMetrics {
    pub sync_latency_histogram: prometheus::Histogram,
    pub sync_size_histogram: prometheus::Histogram,

    pub append_latency_histogram: prometheus::Histogram,

    pub append_log_latency_histogram: prometheus::Histogram,
    pub append_log_throughput_guage: prometheus::Gauge,

    pub batch_writers_histogram: prometheus::Histogram,

    pub block_cache_get_latency_histogram: prometheus::Histogram,
    pub block_cache_insert_latency_histogram: prometheus::Histogram,
    pub block_cache_fill_latency_histogram: prometheus::Histogram,
}

pub type RaftLogStoreMetricsRef = Arc<RaftLogStoreMetrics>;

impl RaftLogStoreMetrics {
    pub fn new(node: u64) -> Self {
        Self {
            sync_latency_histogram: RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["sync", &node.to_string()])
                .unwrap(),
            sync_size_histogram: RAFT_LOG_STORE_SYNC_SIZE_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),

            append_latency_histogram: RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["append", &node.to_string()])
                .unwrap(),

            append_log_latency_histogram: RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["append_log", &node.to_string()])
                .unwrap(),
            append_log_throughput_guage: RAFT_LOG_STORE_THROUGHPUT_GAUGE_VEC
                .get_metric_with_label_values(&["append_log", &node.to_string()])
                .unwrap(),

            block_cache_get_latency_histogram: RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_get", &node.to_string()])
                .unwrap(),
            block_cache_insert_latency_histogram: RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_insert", &node.to_string()])
                .unwrap(),
            block_cache_fill_latency_histogram: RAFT_LOG_STORE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_fill", &node.to_string()])
                .unwrap(),

            batch_writers_histogram: RAFT_LOG_STORE_BATCH_WRITERS_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
        }
    }
}
