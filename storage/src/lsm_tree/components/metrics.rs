use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref INTERNAL_OPS_COUNTER_VEC: prometheus::CounterVec =
        prometheus::register_counter_vec!(
            "lsm_tree_internal_ops_counter_vec",
            "lsm_tree_internal_ops_counter_vec",
            &["op", "node"],
        )
        .unwrap();
    static ref INTERNAL_GAUGE_VEC: prometheus::GaugeVec = prometheus::register_gauge_vec!(
        "lsm_tree_internal_gauge_vec",
        "lsm_tree_internal_gauge_vec",
        &["type", "node"],
    )
    .unwrap();
    static ref BLOCK_CACHE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "lsm_tree_block_cache_latency_histogram_vec",
            "lsm tree block cache latency histogram vec",
            &["op", "node"],
            vec![0.00001, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1]
        )
        .unwrap();
}

pub struct LsmTreeMetrics {
    pub rotate_memtable_counter: prometheus::Counter,
    pub flush_memtable_counter: prometheus::Counter,

    pub active_memtable_size_gauge: prometheus::Gauge,

    pub block_cache_get_latency_histogram: prometheus::Histogram,
    pub block_cache_insert_latency_histogram: prometheus::Histogram,
    pub block_cache_fill_latency_histogram: prometheus::Histogram,
}

pub type LsmTreeMetricsRef = Arc<LsmTreeMetrics>;

impl LsmTreeMetrics {
    pub fn new(node: u64) -> Self {
        Self {
            rotate_memtable_counter: INTERNAL_OPS_COUNTER_VEC
                .get_metric_with_label_values(&["rotate_memtable", &node.to_string()])
                .unwrap(),

            flush_memtable_counter: INTERNAL_OPS_COUNTER_VEC
                .get_metric_with_label_values(&["flush_memtable", &node.to_string()])
                .unwrap(),

            active_memtable_size_gauge: INTERNAL_GAUGE_VEC
                .get_metric_with_label_values(&["active_memtable_size", &node.to_string()])
                .unwrap(),

            block_cache_get_latency_histogram: BLOCK_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_get", &node.to_string()])
                .unwrap(),
            block_cache_insert_latency_histogram: BLOCK_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_insert", &node.to_string()])
                .unwrap(),
            block_cache_fill_latency_histogram: BLOCK_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["block_cache_fill", &node.to_string()])
                .unwrap(),
        }
    }
}
