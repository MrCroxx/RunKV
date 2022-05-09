use std::collections::HashMap;
use std::fs::read_to_string;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures::future;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use runkv_common::log::init_runkv_logger;
use runkv_common::time::timestamp;
use runkv_exhauster::config::ExhausterConfig;
use runkv_exhauster::{bootstrap_exhauster, build_exhauster_with_object_store};
use runkv_proto::common::Endpoint;
use runkv_proto::kv::kv_service_client::KvServiceClient;
use runkv_proto::kv::{DeleteRequest, GetRequest, PutRequest};
use runkv_proto::meta::KeyRange;
use runkv_proto::wheel::wheel_service_client::WheelServiceClient;
use runkv_proto::wheel::{AddEndpointsRequest, AddKeyRangeRequest};
use runkv_rudder::config::RudderConfig;
use runkv_rudder::{bootstrap_rudder, build_rudder_with_object_store};
use runkv_storage::MemObjectStore;
use runkv_wheel::config::WheelConfig;
use runkv_wheel::{bootstrap_wheel, build_wheel_with_object_store};
use tonic::transport::Channel;
use tonic::Request;
use tracing::trace;

const RUDDER_CONFIG_PATH: &str = "bench/etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "bench/etc/wheel.toml";
const EXHAUSTER_CONFIG_PATH: &str = "bench/etc/exhauster.toml";
const LSM_TREE_CONFIG_PATH: &str = "bench/etc/lsm_tree.toml";

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(long, default_value = "1000")]
    concurrenty: u64,
    #[clap(long, default_value = "100")]
    r#loop: u64,
    #[clap(long, default_value = ".run/tmp/bench-kv/raft-log-store-data")]
    raft_log_store_data_dir: String,
    #[clap(long, default_value = ".run/tmp/bench-kv/log")]
    log_dir: String,
}

fn concat_toml(path1: &str, path2: &str) -> String {
    let mut s = String::default();
    s.push_str(&read_to_string(path1).unwrap());
    s.push('\n');
    s.push_str(&read_to_string(path2).unwrap());
    s
}

async fn add_key_range(
    wheel_client: &mut WheelServiceClient<Channel>,
    start: &[u8],
    end: &[u8],
    group: u64,
    raft_nodes: &[u64],
    node: u64,
) {
    wheel_client
        .add_key_range(Request::new(AddKeyRangeRequest {
            key_range: Some(KeyRange {
                start_key: start.to_vec(),
                end_key: end.to_vec(),
            }),
            group,
            raft_nodes: raft_nodes.to_vec(),
            nodes: HashMap::from_iter(raft_nodes.iter().map(|&raft_node| (raft_node, node))),
        }))
        .await
        .unwrap();
}

async fn add_key_ranges(wheel_client: &mut WheelServiceClient<Channel>, node: u64) {
    add_key_range(wheel_client, b"k0", b"k0z", 10, &[11, 12, 13], node).await;
    add_key_range(wheel_client, b"k1", b"k1z", 20, &[21, 22, 23], node).await;
    add_key_range(wheel_client, b"k2", b"k2z", 30, &[31, 32, 33], node).await;
    add_key_range(wheel_client, b"k3", b"k3z", 40, &[41, 42, 43], node).await;
    add_key_range(wheel_client, b"k4", b"k4z", 50, &[51, 52, 53], node).await;
    add_key_range(wheel_client, b"k5", b"k5z", 60, &[61, 62, 63], node).await;
    add_key_range(wheel_client, b"k6", b"k6z", 70, &[71, 72, 73], node).await;
    add_key_range(wheel_client, b"k7", b"k7z", 80, &[81, 82, 83], node).await;
    add_key_range(wheel_client, b"k8", b"k8z", 90, &[91, 92, 93], node).await;
    add_key_range(wheel_client, b"k9", b"k9z", 100, &[101, 102, 103], node).await;
}

async fn mkdir_if_not_exists(path: &str) {
    if tokio::fs::metadata(path).await.is_ok() {
        panic!("path {} already exists", path);
    };
    tokio::fs::create_dir_all(path).await.unwrap();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("{:#?}", args);

    let ts = timestamp();
    let log_dir = format!("{}-{}", args.log_dir, ts);
    let raft_log_store_data_dir = format!("{}-{}", args.raft_log_store_data_dir, ts);

    mkdir_if_not_exists(&log_dir).await;
    mkdir_if_not_exists(&raft_log_store_data_dir).await;

    let _log = init_runkv_logger("tests", 0, &log_dir);

    let rudder_config: RudderConfig =
        toml::from_str(&concat_toml(RUDDER_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();

    let wheel_config: WheelConfig = {
        let mut config: WheelConfig =
            toml::from_str(&concat_toml(WHEEL_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
        config.raft_log_store.log_dir_path = raft_log_store_data_dir;
        config.rudder.port = rudder_config.port;
        config
    };
    let exhauster_config: ExhausterConfig = {
        let mut config: ExhausterConfig =
            toml::from_str(&read_to_string(EXHAUSTER_CONFIG_PATH).unwrap()).unwrap();
        config.rudder.port = rudder_config.port;
        config
    };

    let object_store = Arc::new(MemObjectStore::default());

    let (rudder, rudder_workers) =
        build_rudder_with_object_store(&rudder_config, object_store.clone())
            .await
            .unwrap();

    let (wheel, wheel_workers) = build_wheel_with_object_store(&wheel_config, object_store.clone())
        .await
        .unwrap();

    let (exhuaster, exhauster_workers) =
        build_exhauster_with_object_store(&exhauster_config, object_store)
            .await
            .unwrap();

    tokio::spawn(async move { bootstrap_rudder(&rudder_config, rudder, rudder_workers).await });
    tokio::time::sleep(Duration::from_secs(1)).await;

    tokio::spawn(async move {
        bootstrap_exhauster(&exhauster_config, exhuaster, exhauster_workers).await
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    let wheel_config_clone = wheel_config.clone();
    tokio::spawn(async move { bootstrap_wheel(&wheel_config_clone, wheel, wheel_workers).await });
    tokio::time::sleep(Duration::from_secs(1)).await;

    // TODO: Refine me.
    let mut wheel_client = WheelServiceClient::connect(format!(
        "http://{}:{}",
        wheel_config.host, wheel_config.port
    ))
    .await
    .unwrap();
    wheel_client
        .add_endpoints(AddEndpointsRequest {
            endpoints: HashMap::from_iter([(
                wheel_config.id,
                Endpoint {
                    host: wheel_config.host.to_owned(),
                    port: wheel_config.port as u32,
                },
            )]),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    add_key_ranges(&mut wheel_client, wheel_config.id).await;

    tokio::time::sleep(Duration::from_secs(10)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!(
        "http://{}:{}",
        wheel_config.host, wheel_config.port
    ))
    .unwrap()
    .connect()
    .await
    .unwrap();

    let futures = (0..args.concurrenty)
        .map(|i| {
            let channel_clone = channel.clone();
            async move {
                for _ in 0..args.r#loop {
                    let mut rng = thread_rng();
                    let channel_clone_clone = channel_clone.clone();
                    let mut client = KvServiceClient::new(channel_clone_clone);
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    trace!("put {:?}", key(i));
                    client
                        .put(Request::new(PutRequest {
                            key: key(i),
                            value: value(i),
                        }))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    trace!("get {:?}", key(i));
                    assert_eq!(
                        client
                            .get(Request::new(GetRequest {
                                key: key(i),
                                sequence: 0,
                            }))
                            .await
                            .unwrap()
                            .into_inner()
                            .value,
                        value(i)
                    );
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    trace!("delete {:?}", key(i));
                    client
                        .delete(Request::new(DeleteRequest { key: key(i) }))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    trace!("get {:?}", key(i));
                    assert_eq!(
                        client
                            .get(Request::new(GetRequest {
                                key: key(i),
                                sequence: 0,
                            }))
                            .await
                            .unwrap()
                            .into_inner()
                            .value,
                        vec![]
                    );
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    trace!("put {:?}", key(i));
                    client
                        .put(Request::new(PutRequest {
                            key: key(i),
                            value: value(i),
                        }))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                    trace!("get {:?}", key(i));
                    assert_eq!(
                        client
                            .get(Request::new(GetRequest {
                                key: key(i),
                                sequence: 0,
                            }))
                            .await
                            .unwrap()
                            .into_inner()
                            .value,
                        value(i)
                    );
                }
            }
        })
        .collect_vec();

    let start = Instant::now();

    future::join_all(futures).await;

    println!("elapsed: {:.3?}", start.elapsed());
}

fn key(i: u64) -> Vec<u8> {
    format!("k{:03}{:61}", i, 0).as_bytes().to_vec()
}

fn value(i: u64) -> Vec<u8> {
    format!("v{:03}{:61}", i, 0).as_bytes().to_vec()
}