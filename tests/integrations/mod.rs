#![allow(dead_code)]
#![allow(unused_imports)]

use std::collections::HashMap;
use std::fs::read_to_string;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use runkv_common::log::init_runkv_logger;
use runkv_exhauster::config::ExhausterConfig;
use runkv_exhauster::{bootstrap_exhauster, build_exhauster_with_object_store};
use runkv_proto::common::Endpoint;
use runkv_proto::kv::kv_service_client::KvServiceClient;
use runkv_proto::kv::{DeleteRequest, GetRequest, PutRequest};
use runkv_proto::meta::KeyRange;
use runkv_proto::wheel::wheel_service_client::WheelServiceClient;
use runkv_proto::wheel::{AddEndpointsRequest, AddKeyRangeRequest, InitializeRaftGroupRequest};
use runkv_rudder::config::RudderConfig;
use runkv_rudder::{bootstrap_rudder, build_rudder_with_object_store};
use runkv_storage::MemObjectStore;
use runkv_wheel::config::WheelConfig;
use runkv_wheel::{bootstrap_wheel, build_wheel_with_object_store};
use test_log::test;
use tonic::transport::Channel;
use tonic::Request;
use tracing::trace;

const RUDDER_CONFIG_PATH: &str = "etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "etc/wheel.toml";
const EXHAUSTER_CONFIG_PATH: &str = "etc/exhauster.toml";
const LSM_TREE_CONFIG_PATH: &str = "etc/lsm_tree.toml";

#[tokio::test]
async fn test_concurrent_put_get() {
    init_runkv_logger("runkv_tests");

    let tempdir = tempfile::tempdir().unwrap();
    let raft_log_dir_path = Path::new(tempdir.path())
        .join("raft")
        .to_str()
        .unwrap()
        .to_string();

    let object_store = Arc::new(MemObjectStore::default());

    let rudder_config: RudderConfig =
        toml::from_str(&concat_toml(RUDDER_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
    let (rudder, rudder_workers) =
        build_rudder_with_object_store(&rudder_config, object_store.clone())
            .await
            .unwrap();

    let wheel_config: WheelConfig = {
        let mut config: WheelConfig =
            toml::from_str(&concat_toml(WHEEL_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
        config.raft_log_store.log_dir_path = raft_log_dir_path;
        config
    };
    let (wheel, wheel_workers) = build_wheel_with_object_store(&wheel_config, object_store.clone())
        .await
        .unwrap();

    let exhauster_config: ExhausterConfig =
        toml::from_str(&read_to_string(EXHAUSTER_CONFIG_PATH).unwrap()).unwrap();
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
    wheel_client
        .add_key_range(Request::new(AddKeyRangeRequest {
            key_range: Some(KeyRange {
                start_key: b"k".to_vec(),
                end_key: b"kz".to_vec(),
            }),
            group: 1,
            raft_nodes: vec![1, 2, 3],
            nodes: HashMap::from_iter([
                (1, wheel_config.id),
                (2, wheel_config.id),
                (3, wheel_config.id),
            ]),
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!(
        "http://{}:{}",
        wheel_config.host, wheel_config.port
    ))
    .unwrap()
    .connect()
    .await
    .unwrap();

    let futures = (1..=2000)
        .map(|i| {
            let channel_clone = channel.clone();
            async move {
                let mut rng = thread_rng();
                let mut client = KvServiceClient::new(channel_clone);
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
        })
        .collect_vec();
    future::join_all(futures).await;

    drop(tempdir)
}

fn key(i: u64) -> Vec<u8> {
    format!("k{:064}", i).as_bytes().to_vec()
}

fn value(i: u64) -> Vec<u8> {
    format!("v{:064}", i).as_bytes().to_vec()
}

fn concat_toml(path1: &str, path2: &str) -> String {
    let mut s = String::default();
    s.push_str(&read_to_string(path1).unwrap());
    s.push('\n');
    s.push_str(&read_to_string(path2).unwrap());
    s
}
