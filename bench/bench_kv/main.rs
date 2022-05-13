use runkv_proto::rudder::control_service_client::ControlServiceClient;
use runkv_proto::rudder::{AddKeyRangesRequest, AddWheelsRequest};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
use runkv_proto::meta::{KeyRange, KeyRangeInfo};
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

const RUDDER_NODE_ID: u64 = 10000;
const WHEEL_NODE_ID_BASE: u64 = 0;
const EXHAUSTER_NODE_ID_BASE: u64 = 100;

const RUDDER_PORT: u16 = 12300;
const WHEEL_PORT_BASE: u16 = 12300;
const EXHAUSTER_PORT_BASE: u16 = 12400;

const LOCALHOST: &str = "127.0.0.1";

#[derive(Parser, Debug, Clone)]
struct Args {
    // TODO: Increase default value to 3.
    /// Count of wheel nodes, [1, 10].
    #[clap(long, default_value = "1")]
    wheels: u64,

    /// Count of exhauster nodes, [1, 10].
    #[clap(long, default_value = "1")]
    exhausters: u64,

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

struct ClusterInitializer {
    client: ControlServiceClient<Channel>,
    wheels: HashMap<u64, Endpoint>,
    key_ranges: Vec<KeyRangeInfo>,
}

impl ClusterInitializer {
    fn new(client: ControlServiceClient<Channel>) -> Self {
        Self {
            client,
            wheels: HashMap::default(),
            key_ranges: vec![],
        }
    }

    fn add_wheel(&mut self, node: u64, host: String, port: u16) {
        self.wheels.insert(
            node,
            Endpoint {
                host,
                port: port as u32,
            },
        );
    }

    fn add_key_range(
        &mut self,
        group: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        raft_nodes: HashMap<u64, u64>,
    ) {
        self.key_ranges.push(KeyRangeInfo {
            group,
            key_range: Some(KeyRange { start_key, end_key }),
            raft_nodes,
        });
    }

    async fn init(mut self) {
        let req = AddWheelsRequest {
            wheels: self.wheels,
        };
        self.client.add_wheels(Request::new(req)).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let req = AddKeyRangesRequest {
            key_ranges: self.key_ranges,
        };
        self.client.add_key_ranges(Request::new(req)).await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
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
    // Parse arguments.
    let args = Args::parse();
    println!("{:#?}", args);

    // Prepare directories.
    println!("Prepare directories...");
    let ts = timestamp();
    let log_dir = format!("{}-{}", args.log_dir, ts);
    let raft_log_store_data_dir = format!("{}-{}", args.raft_log_store_data_dir, ts);
    mkdir_if_not_exists(&log_dir).await;
    mkdir_if_not_exists(&raft_log_store_data_dir).await;

    // Init log.
    println!("Init log...");
    let _log = init_runkv_logger("tests", 0, &log_dir);

    // Read config templates.
    println!("Read config templates...");
    let rudder_config: RudderConfig = {
        let mut config: RudderConfig =
            toml::from_str(&concat_toml(RUDDER_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
        config.id = RUDDER_NODE_ID;
        config.host = LOCALHOST.to_string();
        config.port = RUDDER_PORT;
        config
    };
    let wheel_config_template: WheelConfig = {
        let mut config: WheelConfig =
            toml::from_str(&concat_toml(WHEEL_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
        config.rudder.id = RUDDER_NODE_ID;
        config.rudder.host = rudder_config.host.clone();
        config.rudder.port = rudder_config.port;
        config.raft_log_store.persist = args.persist;
        config.host = LOCALHOST.to_string();
        config
    };
    let exhauster_config_template: ExhausterConfig = {
        let mut config: ExhausterConfig =
            toml::from_str(&read_to_string(EXHAUSTER_CONFIG_PATH).unwrap()).unwrap();
        config.rudder.id = RUDDER_NODE_ID;
        config.rudder.host = rudder_config.host.clone();
        config.rudder.port = rudder_config.port;
        config.host = LOCALHOST.to_string();
        config
    };

    // Connect object store.
    // TODO: Support S3.
    println!("Connect object store...");
    let object_store = Arc::new(MemObjectStore::default());

    // Build and bootstrap rudder.
    println!("Bootstrap rudder...");
    let (rudder, rudder_workers) =
        build_rudder_with_object_store(&rudder_config, object_store.clone())
            .await
            .unwrap();
    let rudder_config_clone = rudder_config.clone();
    tokio::spawn(
        async move { bootstrap_rudder(&rudder_config_clone, rudder, rudder_workers).await },
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Build and bootstrap wheels.
    for i in 1..=args.wheels {
        println!("Bootstrap wheel {}...", i);
        let wheel_config = {
            let mut config = wheel_config_template.clone();
            config.raft_log_store.log_dir_path = format!("{}/{}", raft_log_store_data_dir, i);
            config.id = i + WHEEL_NODE_ID_BASE;
            config.port = i as u16 + WHEEL_PORT_BASE;
            config
        };
        let (wheel, wheel_workers) =
            build_wheel_with_object_store(&wheel_config, object_store.clone())
                .await
                .unwrap();
        tokio::spawn(async move { bootstrap_wheel(&wheel_config, wheel, wheel_workers).await });
    }

    // Build and bootstrap exhausters.
    for i in 1..=args.exhausters {
        println!("Bootstrap exhauster {}...", i);
        let exhauster_config = {
            let mut config = exhauster_config_template.clone();
            config.id = i + EXHAUSTER_NODE_ID_BASE;
            config.port = i as u16 + EXHAUSTER_PORT_BASE;
            config
        };
        let (exhuaster, exhauster_workers) =
            build_exhauster_with_object_store(&exhauster_config, object_store.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            bootstrap_exhauster(&exhauster_config, exhuaster, exhauster_workers).await
        });
    }

    // Initialize cluster.
    println!("Init cluster...");
    let client =
        ControlServiceClient::connect(format!("http://{}:{}", LOCALHOST, rudder_config.port))
            .await
            .unwrap();
    let mut initializer = ClusterInitializer::new(client);
    for i in 1..=args.wheels {
        initializer.add_wheel(
            i + WHEEL_NODE_ID_BASE,
            LOCALHOST.to_string(),
            i as u16 + WHEEL_PORT_BASE,
        );
    }
    for (i, group) in (1..=args.groups).enumerate() {
        let raft_nodes = HashMap::from_iter([
            (
                (i as u64) * 3 + 1,
                (((i as u64) * 3) % args.wheels as u64) + 1 + WHEEL_NODE_ID_BASE,
            ),
            (
                (i as u64) * 3 + 2,
                (((i as u64) * 3 + 1) % args.wheels as u64) + 1 + WHEEL_NODE_ID_BASE,
            ),
            (
                (i as u64) * 3 + 3,
                (((i as u64) * 3 + 2) % args.wheels as u64) + 1 + WHEEL_NODE_ID_BASE,
            ),
        ]);
        let start_key = start_key(group);
        let end_key = end_key(group);
        println!(
            "Add key range: [group: {}] [start key: {:?}] [end key: {:?}] [raft nodes: {:?}]",
            group, start_key, end_key, raft_nodes,
        );
        initializer.add_key_range(group as u64, start_key, end_key, raft_nodes);
    }
    initializer.init().await;

    let channel = tonic::transport::Endpoint::from_shared(format!(
        "http://{}:{}",
        // TODO: Support multi wheels.
        LOCALHOST,
        1 + WHEEL_PORT_BASE,
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
