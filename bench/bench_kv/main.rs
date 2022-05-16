#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::Parser;
use runkv_tests::{run, Args, Options};

const RUDDER_CONFIG_PATH: &str = "bench/etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "bench/etc/wheel.toml";
const EXHAUSTER_CONFIG_PATH: &str = "bench/etc/exhauster.toml";
const LSM_TREE_CONFIG_PATH: &str = "bench/etc/lsm_tree.toml";

const RUDDER_NODE_ID: u64 = 10000;
const WHEEL_NODE_ID_BASE: u64 = 0;
const EXHAUSTER_NODE_ID_BASE: u64 = 100;

const RUDDER_PORT: u16 = 12300;
const WHEEL_PORT_BASE: u16 = 12300;
const WHEEL_PROMETHEUS_PORT_BASE: u16 = 9890;
const EXHAUSTER_PORT_BASE: u16 = 12400;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:#?}", args);

    let options = Options {
        log: true,
        rudder_config_path: RUDDER_CONFIG_PATH.to_string(),
        wheel_config_path: WHEEL_CONFIG_PATH.to_string(),
        exhauster_config_path: EXHAUSTER_CONFIG_PATH.to_string(),
        lsm_tree_config_path: LSM_TREE_CONFIG_PATH.to_string(),
        rudder_node_id: RUDDER_NODE_ID,
        wheel_node_id_base: WHEEL_NODE_ID_BASE,
        exhauster_node_id_base: EXHAUSTER_NODE_ID_BASE,
        rudder_port: RUDDER_PORT,
        wheel_port_base: WHEEL_PORT_BASE,
        wheel_prometheus_port_base: WHEEL_PROMETHEUS_PORT_BASE,
        exhauster_port_base: EXHAUSTER_PORT_BASE,
    };
    println!("{:#?}", options);

    run(args, options).await;
}
