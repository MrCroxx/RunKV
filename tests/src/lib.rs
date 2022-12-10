use std::collections::HashMap;
use std::fs::read_to_string;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BufMut;
use clap::Parser;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use runkv_client::client::{RunkvClient, RunkvClientOptions};
use runkv_common::log::init_runkv_logger;
use runkv_common::time::timestamp;
use runkv_exhauster::config::ExhausterConfig;
use runkv_exhauster::{bootstrap_exhauster, build_exhauster_with_object_store};
use runkv_proto::common::Endpoint;
use runkv_proto::meta::{KeyRange, KeyRangeInfo};
use runkv_proto::rudder::control_service_client::ControlServiceClient;
use runkv_proto::rudder::{AddKeyRangesRequest, AddWheelsRequest};
use runkv_rudder::config::RudderConfig;
use runkv_rudder::{bootstrap_rudder, build_rudder_with_object_store};
use runkv_storage::{MemObjectStore, ObjectStore, S3ObjectStore};
use runkv_wheel::config::{TieredCacheConfig, WheelConfig};
use runkv_wheel::{bootstrap_wheel, build_wheel_with_object_store};
use tonic::transport::Channel;
use tonic::Request;

const LOCALHOST: &str = "127.0.0.1";

#[derive(Clone, Debug)]
pub struct Options {
    pub log: bool,
    pub rudder_config_path: String,
    pub wheel_config_path: String,
    pub exhauster_config_path: String,
    pub lsm_tree_config_path: String,
    pub rudder_node_id: u64,
    pub wheel_node_id_base: u64,
    pub exhauster_node_id_base: u64,
    pub rudder_port: u16,
    pub wheel_port_base: u16,
    pub wheel_prometheus_port_base: u16,
    pub exhauster_port_base: u16,
}

#[derive(Parser, Clone, Debug)]
pub struct Args {
    /// Count of wheel nodes, [1, 10].
    #[clap(long, default_value = "1")]
    pub wheels: u64,

    /// Count of exhauster nodes, [1, 10].
    #[clap(long, default_value = "1")]
    pub exhausters: u64,

    /// Count of raft groups, [1, 100].
    #[clap(long, default_value = "10")]
    pub groups: u8,

    /// Key size (B), [10, 4096].
    #[clap(long, default_value = "64")]
    pub key_size: usize,

    /// Valuw size (B), [10, 4096].
    #[clap(long, default_value = "64")]
    pub value_size: usize,

    /// Concurrency of each raft group.
    #[clap(long, default_value = "100")]
    pub concurrency: u64,

    /// Loop time of each coroutine.
    #[clap(long, default_value = "100")]
    pub r#loop: u64,

    #[clap(long, default_value = ".run/tmp/bench-kv/raft-log-store-data")]
    pub raft_log_store_data_dir: String,

    #[clap(long, default_value = "sync")]
    pub persist: String,

    #[clap(long, default_value = ".run/tmp/bench-kv/log")]
    pub log_dir: String,

    #[clap(long, default_value = ".run/tmp/bench-kv/filecache")]
    pub file_cache_dir: String,

    /// 1. memory => "memory://" 2. minio => "minio://key:secret@address:port/bucket"
    #[clap(long, default_value = "memory://")]
    pub s3_uri: String,

    #[clap(long, default_value = "1.0")]
    pub mc: f64,
}

fn concat_toml(path1: &str, path2: &str) -> String {
    let mut s = String::default();
    s.push_str(&read_to_string(path1).unwrap());
    s.push('\n');
    s.push_str(&read_to_string(path2).unwrap());
    s
}

#[allow(clippy::uninit_vec)]
fn key(group: u8, i: u64, size: usize, rng: &mut impl Rng) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    unsafe {
        buf.set_len(size);
    }
    rng.fill(&mut buf[..]);
    (&mut buf[0..1]).put_u8(b'k');
    (&mut buf[1..2]).put_u8(group);
    (&mut buf[2..10]).put_u64(i);
    buf
}

#[allow(clippy::uninit_vec)]
fn value(group: u8, i: u64, size: usize, rng: &mut impl Rng) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    unsafe {
        buf.set_len(size);
    }
    rng.fill(&mut buf[..]);
    (&mut buf[0..1]).put_u8(b'v');
    (&mut buf[1..2]).put_u8(group);
    (&mut buf[2..10]).put_u64(i);
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
            leader: 0,
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

enum ObjectStoreFactory {
    Memory(Arc<MemObjectStore>),
    Minio(String),
}

impl ObjectStoreFactory {
    async fn produce(&self) -> Arc<dyn ObjectStore> {
        match self {
            Self::Memory(mem) => mem.clone(),
            Self::Minio(uri) => Arc::new(S3ObjectStore::new_with_minio(uri).await),
        }
    }
}

async fn assert_put(client: &RunkvClient, key: Vec<u8>, value: Vec<u8>) {
    client.put(key, value).await.unwrap();
}

async fn assert_get(client: &RunkvClient, key: Vec<u8>, expected: Option<Vec<u8>>) {
    let result = client.get(key).await.unwrap();
    assert_eq!(result, expected);
}

async fn assert_delete(client: &RunkvClient, key: Vec<u8>) {
    client.delete(key).await.unwrap();
}

async fn mkdir_if_not_exists(path: &str) {
    if tokio::fs::metadata(path).await.is_ok() {
        panic!("path {} already exists", path);
    };
    tokio::fs::create_dir_all(path).await.unwrap();
}

pub async fn run(args: Args, options: Options) {
    #[cfg(feature = "deadlock")]
    {
        // Create a background thread which checks for deadlocks every 10s
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{}", i);
                for t in threads {
                    println!("Thread Id {:#?}", t.thread_id());
                    println!("{:#?}", t.backtrace());
                }
            }
            panic!("Deadlocks detected!");
        });
    }

    // Prepare directories.
    println!("Prepare directories...");
    let ts = timestamp();
    let log_dir = format!("{}-{}", args.log_dir, ts);
    let raft_log_store_data_dir = format!("{}-{}", args.raft_log_store_data_dir, ts);
    let file_cache_dir = format!("{}-{}", args.file_cache_dir, ts);
    mkdir_if_not_exists(&log_dir).await;
    mkdir_if_not_exists(&raft_log_store_data_dir).await;
    mkdir_if_not_exists(&file_cache_dir).await;

    // Init log.
    println!("Init log...");
    let log_guard = if options.log {
        Some(init_runkv_logger("tests", 0, &log_dir))
    } else {
        None
    };

    // Read config templates.
    println!("Read config templates...");
    let rudder_config: RudderConfig = {
        let mut config: RudderConfig = toml::from_str(&concat_toml(
            &options.rudder_config_path,
            &options.lsm_tree_config_path,
        ))
        .unwrap();
        config.id = options.rudder_node_id;
        config.host = LOCALHOST.to_string();
        config.port = options.rudder_port;
        config
    };
    let wheel_config_template: WheelConfig = {
        let mut config: WheelConfig = toml::from_str(&concat_toml(
            &options.wheel_config_path,
            &options.lsm_tree_config_path,
        ))
        .unwrap();
        config.rudder.id = options.rudder_node_id;
        config.rudder.host = rudder_config.host.clone();
        config.rudder.port = rudder_config.port;
        config.raft_log_store.persist = args.persist;
        config.host = LOCALHOST.to_string();
        if let TieredCacheConfig::FileCache(cfg) = &mut config.tiered_cache {
            cfg.dir = file_cache_dir.to_owned();
        }
        config
    };
    let exhauster_config_template: ExhausterConfig = {
        let mut config: ExhausterConfig =
            toml::from_str(&read_to_string(options.exhauster_config_path).unwrap()).unwrap();
        config.rudder.id = options.rudder_node_id;
        config.rudder.host = rudder_config.host.clone();
        config.rudder.port = rudder_config.port;
        config.host = LOCALHOST.to_string();
        config
    };

    // Parse object store args.
    let object_store_factory = match args.s3_uri.split("://").collect_vec().as_slice() {
        ["memory", ..] => ObjectStoreFactory::Memory(Arc::new(MemObjectStore::default())),
        ["minio", ..] => ObjectStoreFactory::Minio(args.s3_uri.clone()),
        [args @ ..] => panic!("not valid s3 url: {:?}", args),
    };

    // Build and bootstrap rudder.
    println!("Bootstrap rudder...");
    let (rudder, rudder_workers) =
        build_rudder_with_object_store(&rudder_config, object_store_factory.produce().await)
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
            if let TieredCacheConfig::FileCache(cfg) = &mut config.tiered_cache {
                cfg.dir = format!("{}/{}", cfg.dir, i);
            }
            config.id = i + options.wheel_node_id_base;
            config.port = i as u16 + options.wheel_port_base;
            config.prometheus.port = i as u16 + options.wheel_prometheus_port_base;
            config
        };
        let (wheel, wheel_workers) =
            build_wheel_with_object_store(&wheel_config, object_store_factory.produce().await)
                .await
                .unwrap();
        tokio::spawn(async move { bootstrap_wheel(&wheel_config, wheel, wheel_workers).await });
    }

    // Build and bootstrap exhausters.
    for i in 1..=args.exhausters {
        println!("Bootstrap exhauster {}...", i);
        let exhauster_config = {
            let mut config = exhauster_config_template.clone();
            config.id = i + options.exhauster_node_id_base;
            config.port = i as u16 + options.exhauster_port_base;
            config
        };
        let (exhuaster, exhauster_workers) = build_exhauster_with_object_store(
            &exhauster_config,
            object_store_factory.produce().await,
        )
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
            i + options.wheel_node_id_base,
            LOCALHOST.to_string(),
            i as u16 + options.wheel_port_base,
        );
    }
    for (i, group) in (1..=args.groups).enumerate() {
        let raft_nodes = HashMap::from_iter([
            (
                (i as u64) * 3 + 1,
                (((i as u64) * 3) % args.wheels) + 1 + options.wheel_node_id_base,
            ),
            (
                (i as u64) * 3 + 2,
                (((i as u64) * 3 + 1) % args.wheels) + 1 + options.wheel_node_id_base,
            ),
            (
                (i as u64) * 3 + 3,
                (((i as u64) * 3 + 2) % args.wheels) + 1 + options.wheel_node_id_base,
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

    let runkv_client = RunkvClient::open(RunkvClientOptions {
        rudder: rudder_config.id,
        rudder_host: rudder_config.host.clone(),
        rudder_port: rudder_config.port,
        heartbeat_interval: Duration::from_nanos(0),
    })
    .await;
    runkv_client.update_router().await.unwrap();

    let futures = (1..=args.groups)
        .flat_map(|group| {
            let runkv_client_clone = runkv_client.clone();
            (1..=args.concurrency).map(move |c| {
                let client_clone = runkv_client_clone.clone();
                async move {
                    let mut rng = StdRng::seed_from_u64(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64,
                    );

                    let client = client_clone.clone();

                    let key = key(group, c, args.key_size, &mut rng);
                    let value = value(group, c, args.value_size, &mut rng);

                    for _ in 0..args.r#loop {
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_put(&client, key.clone(), value.clone()).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_get(&client, key.clone(), Some(value.clone())).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_delete(&client, key.clone()).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_get(&client, key.clone(), None).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_put(&client, key.clone(), value.clone()).await;
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                        assert_get(&client, key.clone(), Some(value.clone())).await;
                    }
                }
            })
        })
        .collect_vec();

    let start = Instant::now();

    println!("Start kv operations...");

    let hmock = tokio::spawn(mock(args.mc));

    let handles = futures
        .into_iter()
        .map(|future| tokio::spawn(future))
        .collect_vec();
    for handle in handles {
        handle.await.unwrap();
    }

    println!("elapsed: {:.3?}", start.elapsed());
    println!("Finish.");

    hmock.abort();

    drop(log_guard);
}

use lazy_static::lazy_static;

lazy_static! {
    static ref COST: prometheus::Counter = prometheus::register_counter!("cost", "cost").unwrap();
}

async fn mock(c: f64) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        COST.inc_by(c);
        tokio::task::consume_budget().await;
    }
}
