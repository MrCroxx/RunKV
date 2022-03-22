use std::fs::read_to_string;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use runkv_exhauster::config::ExhausterConfig;
use runkv_exhauster::{bootstrap_exhauster, build_exhauster_with_object_store};
use runkv_proto::meta::KeyRange;
use runkv_proto::wheel::wheel_service_client::WheelServiceClient;
use runkv_rudder::config::RudderConfig;
use runkv_rudder::{bootstrap_rudder, build_rudder_with_object_store};
use runkv_storage::{LsmTree, MemObjectStore};
use runkv_wheel::config::WheelConfig;
use runkv_wheel::{bootstrap_wheel, build_wheel_with_object_store};
use tonic::Request;

const RUDDER_CONFIG_PATH: &str = "etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "etc/wheel.toml";
const EXHAUSTER_CONFIG_PATH: &str = "etc/exhauster.toml";
const LSM_TREE_CONFIG_PATH: &str = "etc/lsm_tree.toml";

#[tokio::test]
async fn test_concurrent_put_get() {
    let object_store = Arc::new(MemObjectStore::default());

    let rudder_config: RudderConfig =
        toml::from_str(&concat_toml(RUDDER_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
    let (rudder, rudder_workers) =
        build_rudder_with_object_store(&rudder_config, object_store.clone())
            .await
            .unwrap();
    tokio::spawn(async move { bootstrap_rudder(&rudder_config, rudder, rudder_workers).await });
    tokio::time::sleep(Duration::from_secs(3)).await;

    let wheel_config: WheelConfig =
        toml::from_str(&concat_toml(WHEEL_CONFIG_PATH, LSM_TREE_CONFIG_PATH)).unwrap();
    let (wheel, lsmtree, wheel_workers) =
        build_wheel_with_object_store(&wheel_config, object_store.clone())
            .await
            .unwrap();
    let wheel_config_clone = wheel_config.clone();
    tokio::spawn(async move { bootstrap_wheel(&wheel_config_clone, wheel, wheel_workers).await });
    tokio::time::sleep(Duration::from_secs(3)).await;

    let exhauster_config: ExhausterConfig =
        toml::from_str(&read_to_string(EXHAUSTER_CONFIG_PATH).unwrap()).unwrap();
    let (exhuaster, exhauster_workers) =
        build_exhauster_with_object_store(&exhauster_config, object_store)
            .await
            .unwrap();

    tokio::spawn(async move {
        bootstrap_exhauster(&exhauster_config, exhuaster, exhauster_workers).await
    });
    tokio::time::sleep(Duration::from_secs(3)).await;

    // TODO: Refine me.
    let mut wheel_client = WheelServiceClient::connect(format!(
        "http://{}:{}",
        wheel_config.host, wheel_config.port
    ))
    .await
    .unwrap();
    wheel_client
        .update_key_ranges(Request::new(runkv_proto::wheel::UpdateKeyRangesRequest {
            key_ranges: vec![KeyRange {
                start_key: b"k".to_vec(),
                end_key: b"kz".to_vec(),
            }],
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let futures = (1..=10000)
        .map(|i| {
            let lsmtree_clone = lsmtree.clone();
            async move {
                let mut rng = thread_rng();
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                lsmtree_clone.put(&key(i), &value(i), 1).await.unwrap();
                // println!("put {:?} at {}", key(i), 1);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                // println!("get {:?} at {}", key(i), 3);
                assert_eq!(lsmtree_clone.get(&key(i), 3).await.unwrap(), Some(value(i)));
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                lsmtree_clone.delete(&key(i), 5).await.unwrap();
                // println!("delete {:?} at {}", key(i), 5);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                // println!("get {:?} at {}", key(i), 7);
                assert_eq!(lsmtree_clone.get(&key(i), 7).await.unwrap(), None);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                lsmtree_clone.put(&key(i), &value(i), 9).await.unwrap();
                // println!("put {:?} at {}", key(i), 9);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
                // println!("get {:?} at {}", key(i), 11);
                assert_eq!(
                    lsmtree_clone.get(&key(i), 11).await.unwrap(),
                    Some(value(i))
                );
            }
        })
        .collect_vec();
    future::join_all(futures).await;
    while lsmtree.get_oldest_immutable_memtable().is_some() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn key(i: u64) -> Bytes {
    Bytes::from(format!("k{:064}", i))
}

fn value(i: u64) -> Bytes {
    Bytes::from(format!("v{:064}", i))
}

fn concat_toml(path1: &str, path2: &str) -> String {
    let mut s = String::default();
    s.push_str(&read_to_string(path1).unwrap());
    s.push('\n');
    s.push_str(&read_to_string(path2).unwrap());
    s
}
