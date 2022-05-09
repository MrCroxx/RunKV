use std::collections::HashMap;
use std::fs::read_to_string;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BufMut;
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

const RUDDER_CONFIG_PATH: &str = "bench/etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "bench/etc/wheel.toml";
const EXHAUSTER_CONFIG_PATH: &str = "bench/etc/exhauster.toml";
const LSM_TREE_CONFIG_PATH: &str = "bench/etc/lsm_tree.toml";

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Count of raft groups, [1, 100].
    #[clap(long, default_value = "10")]
    groups: u8,

    /// Key size (B), [10, 4096].
    #[clap(long, default_value = "64")]
    key_size: usize,

    /// Valuw size (B), [10, 4096].
    #[clap(long, default_value = "64")]
    value_size: usize,

    /// Concurrency of each raft group.
    #[clap(long, default_value = "100")]
    concurrency: u64,

    /// Loop time of each coroutine.
    #[clap(long, default_value = "100")]
    r#loop: u64,

    #[clap(long, default_value = ".run/tmp/bench-kv/raft-log-store-data")]
    raft_log_store_data_dir: String,

    #[clap(long, default_value = "sync")]
    persist: String,

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

fn key(group: u8, i: u64, size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    buf.put_u8(b'k');
    buf.put_u8(group);
    buf.put_u64(i);
    buf.put_slice(&vec![b' '; size - 10]);
    buf
}

fn value(group: u8, i: u64, size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    buf.put_u8(b'v');
    buf.put_u8(group);
    buf.put_u64(i);
    buf.put_slice(&vec![b' '; size - 10]);
    buf
}

fn start_key(group: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2);
    buf.put_u8(b'k');
    buf.put_u8(group);
    buf
}

fn end_key(group: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3);
    buf.put_u8(b'k');
    buf.put_u8(group);
    buf.put_u8(255);
    buf
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

async fn assert_put(client: &mut KvServiceClient<Channel>, key: Vec<u8>, value: Vec<u8>) {
    client
        .put(Request::new(PutRequest { key, value }))
        .await
        .unwrap();
}

async fn assert_get(
    client: &mut KvServiceClient<Channel>,
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
) {
    let result = client
        .get(Request::new(GetRequest { key, sequence: 0 }))
        .await
        .unwrap()
        .into_inner()
        .value;
    let result = if result.is_empty() {
        None
    } else {
        Some(result)
    };
    assert_eq!(result, expected);
}

async fn assert_delete(client: &mut KvServiceClient<Channel>, key: Vec<u8>) {
    client
        .delete(Request::new(DeleteRequest { key }))
        .await
        .unwrap();
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
        config.raft_log_store.persist = args.persist;
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

    // Add key ranges.
    println!("Add key ranges...");
    for group in 1..=args.groups {
        add_key_range(
            &mut wheel_client,
            &start_key(group),
            &end_key(group),
            group as u64,
            &[
                (group as u64 - 1) * 3 + 1,
                (group as u64 - 1) * 3 + 2,
                (group as u64 - 1) * 3 + 3,
            ],
            wheel_config.id,
        )
        .await;
    }

    tokio::time::sleep(Duration::from_secs(10)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!(
        "http://{}:{}",
        wheel_config.host, wheel_config.port
    ))
    .unwrap()
    .connect()
    .await
    .unwrap();

    let futures = (1..=args.groups)
        .flat_map(|group| {
            let channel_clone = channel.clone();
            (1..=args.concurrency).map(move |c| {
                let channel_clone_clone = channel_clone.clone();
                async move {
                    let mut rng = thread_rng();

                    let mut client = KvServiceClient::new(channel_clone_clone);

                    let key = key(group, c, args.key_size);
                    let value = value(group, c, args.value_size);

                    for _ in 0..args.r#loop {
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_put(&mut client, key.clone(), value.clone()).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_get(&mut client, key.clone(), Some(value.clone())).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_delete(&mut client, key.clone()).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_get(&mut client, key.clone(), None).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_put(&mut client, key.clone(), value.clone()).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_get(&mut client, key.clone(), Some(value.clone())).await;
                    }
                }
            })
        })
        .collect_vec();

    let start = Instant::now();

    println!("Start benching...");

    future::join_all(futures).await;

    println!("elapsed: {:.3?}", start.elapsed());
    println!("Finish.");
}
