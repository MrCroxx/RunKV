use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref BLOCK_CACHE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "block_cache_latency_histogram_vec",
            "block_cache latency histogram vec",
            &["op", "node"]
        )
        .unwrap();
}

pub struct LsmTreeMetrics {
    pub block_cache_get_latency_histogram: prometheus::Histogram,
    pub block_cache_insert_latency_histogram: prometheus::Histogram,
    pub block_cache_fill_latency_histogram: prometheus::Histogram,
}

pub type LsmTreeMetricsRef = Arc<LsmTreeMetrics>;

impl LsmTreeMetrics {
    pub fn new(node: u64) -> Self {
        Self {
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
