// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref FILE_CACHE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "file_cache_latency_histogram_vec",
            "file cache latency histogram vec",
            &["op", "node"],
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
                1.0
            ],
        )
        .unwrap();
    static ref FILE_CACHE_MISS_COUNTER: prometheus::IntCounterVec =
        prometheus::register_int_counter_vec!("file_cache_miss", "file cache miss", &["node"])
            .unwrap();
    static ref FILE_CACHE_DISK_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "file_cache_disk_latency_histogram_vec",
            "file cache disk latency histogram vec",
            &["op", "node"],
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
                1.0
            ],
        )
        .unwrap();
    static ref FILE_CACHE_DISK_COUNTER_VEC: prometheus::CounterVec =
        prometheus::register_counter_vec!(
            "file_cache_disk_counter_vec",
            "file cache disk counter vec",
            &["op", "node"],
        )
        .unwrap();
    static ref FILE_CACHE_DISK_IO_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "file_cache_disk_io_histogram_vec",
            "file cache disk io histogram vec",
            &["op", "node"],
        )
        .unwrap();
}

pub struct FileCacheMetrics {
    pub cache_miss: prometheus::IntCounter,

    pub insert_latency: prometheus::Histogram,
    pub erase_latency: prometheus::Histogram,
    pub get_latency: prometheus::Histogram,

    pub disk_read_bytes: prometheus::Counter,
    pub disk_read_latency: prometheus::Histogram,
    pub disk_write_bytes: prometheus::Counter,
    pub disk_write_latency: prometheus::Histogram,
    pub disk_read_io_size: prometheus::Histogram,
    pub disk_write_io_size: prometheus::Histogram,
}

impl FileCacheMetrics {
    pub fn new(node: u64) -> Self {
        Self {
            cache_miss: FILE_CACHE_MISS_COUNTER
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),

            insert_latency: FILE_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["insert", &node.to_string()])
                .unwrap(),
            erase_latency: FILE_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["erase", &node.to_string()])
                .unwrap(),
            get_latency: FILE_CACHE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["get", &node.to_string()])
                .unwrap(),

            disk_read_bytes: FILE_CACHE_DISK_COUNTER_VEC
                .get_metric_with_label_values(&["read", &node.to_string()])
                .unwrap(),
            disk_read_latency: FILE_CACHE_DISK_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["read", &node.to_string()])
                .unwrap(),
            disk_write_bytes: FILE_CACHE_DISK_COUNTER_VEC
                .get_metric_with_label_values(&["write", &node.to_string()])
                .unwrap(),
            disk_write_latency: FILE_CACHE_DISK_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["write", &node.to_string()])
                .unwrap(),
            disk_read_io_size: FILE_CACHE_DISK_IO_HISTOGRAM_VEC
                .get_metric_with_label_values(&["read", &node.to_string()])
                .unwrap(),
            disk_write_io_size: FILE_CACHE_DISK_IO_HISTOGRAM_VEC
                .get_metric_with_label_values(&["write", &node.to_string()])
                .unwrap(),
        }
    }
}

pub type FileCacheMetricsRef = Arc<FileCacheMetrics>;
