use std::path::Path;

use runkv_tests::{run, Args, Options};
use test_log::test;

use crate::*;

#[test(tokio::test)]
async fn test_multi_raft_group_concurrent_put_get() {
    let port = crate::port("test_multi_raft_group_concurrent_put_get");

    let options = Options {
        log: false,
        rudder_config_path: RUDDER_CONFIG_PATH.to_string(),
        wheel_config_path: WHEEL_CONFIG_PATH.to_string(),
        exhauster_config_path: EXHAUSTER_CONFIG_PATH.to_string(),
        lsm_tree_config_path: LSM_TREE_CONFIG_PATH.to_string(),
        rudder_node_id: 10000,
        wheel_node_id_base: 0,
        exhauster_node_id_base: 100,
        rudder_port: port,
        exhauster_port_base: port,
        wheel_port_base: port + 1,
        wheel_prometheus_port_base: 0,
    };

    let tempdir = tempfile::tempdir().unwrap();
    let raft_log_store_data_dir = Path::new(tempdir.path())
        .join("raft")
        .to_str()
        .unwrap()
        .to_string();
    let log_dir = Path::new(tempdir.path())
        .join("log")
        .to_str()
        .unwrap()
        .to_string();

    let args = Args {
        wheels: 1,
        exhausters: 1,
        groups: 10,
        key_size: 64,
        value_size: 64,
        concurrency: 100,
        r#loop: 3,
        raft_log_store_data_dir,
        persist: "sync".to_string(),
        log_dir,
    };

    run(args, options).await;
}
