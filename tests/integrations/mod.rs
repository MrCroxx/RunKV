use std::fs::read_to_string;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use runkv_rudder::config::RudderConfig;
use runkv_rudder::{bootstrap_rudder, build_rudder_with_object_store};
use runkv_storage::MemObjectStore;
use runkv_wheel::config::WheelConfig;
use runkv_wheel::lsm_tree::LsmTree;
use runkv_wheel::{bootstrap_wheel, build_wheel_with_object_store};

const RUDDER_CONFIG_PATH: &str = "etc/rudder.toml";
const WHEEL_CONFIG_PATH: &str = "etc/wheel.toml";

#[tokio::test]
async fn test_concurrent_put_get() {
    let object_store = Arc::new(MemObjectStore::default());

    let rudder_config: RudderConfig =
        toml::from_str(&read_to_string(RUDDER_CONFIG_PATH).unwrap()).unwrap();
    let rudder = build_rudder_with_object_store(&rudder_config, object_store.clone())
        .await
        .unwrap();
    tokio::spawn(async move { bootstrap_rudder(&rudder_config, rudder).await });
    tokio::time::sleep(Duration::from_secs(3)).await;

    let wheel_config: WheelConfig =
        toml::from_str(&read_to_string(WHEEL_CONFIG_PATH).unwrap()).unwrap();
    let (wheel, lsmtree) = build_wheel_with_object_store(&wheel_config, object_store)
        .await
        .unwrap();
    tokio::spawn(async move { bootstrap_wheel(&wheel_config, wheel).await });
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

// use std::sync::Arc;
// use std::time::Duration;

// use bytes::Bytes;
// use futures::future;
// use itertools::Itertools;
// use rand::{thread_rng, Rng};
// use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions};
// use runkv_storage::manifest::{
//     LevelCompactionStrategy, LevelOptions, VersionManager, VersionManagerOptions,
// };
// use runkv_storage::utils::CompressionAlgorithm;
// use runkv_storage::MemObjectStore;
// use runkv_wheel::lsm_tree::sstable_uploader::{SstableUploader, SstableUploaderOptions};
// use runkv_wheel::lsm_tree::{LsmTree, ObjectStoreLsmTree, ObjectStoreLsmTreeOptions};

// #[tokio::test]
// async fn test_concurrent_put() {
//     let lsmtree = default_lsm_tree_for_test().await;
//     let futures = (1..=10000)
//         .map(|i| {
//             let lsmtree_clone = lsmtree.clone();
//             async move {
//                 let mut rng = thread_rng();
//                 tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
//                 lsmtree_clone.put(&key(i), &value(i), 1).await.unwrap();
//                 // println!("put {:?} at {}", key(i), 1);
//                 tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
//                 // println!("get {:?} at {}", key(i), 3);
//                 assert_eq!(lsmtree_clone.get(&key(i), 3).await.unwrap(), Some(value(i)));
//                 tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
//                 lsmtree_clone.delete(&key(i), 5).await.unwrap();
//                 // println!("delete {:?} at {}", key(i), 5);
//                 tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
//                 // println!("get {:?} at {}", key(i), 7);
//                 assert_eq!(lsmtree_clone.get(&key(i), 7).await.unwrap(), None);
//                 tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
//                 lsmtree_clone.put(&key(i), &value(i), 9).await.unwrap();
//                 // println!("put {:?} at {}", key(i), 9);
//                 tokio::time::sleep(Duration::from_millis(rng.gen_range(0..100))).await;
//                 // println!("get {:?} at {}", key(i), 11);
//                 assert_eq!(
//                     lsmtree_clone.get(&key(i), 11).await.unwrap(),
//                     Some(value(i))
//                 );
//             }
//         })
//         .collect_vec();
//     future::join_all(futures).await;
//     while lsmtree.get_oldest_immutable_memtable().is_some() {
//         tokio::time::sleep(Duration::from_millis(200)).await;
//     }
// }

// async fn default_lsm_tree_for_test() -> ObjectStoreLsmTree {
//     let object_store = Arc::new(MemObjectStore::default());
//     let block_cache = BlockCache::new(65536);
//     let sstable_store_options = SstableStoreOptions {
//         path: "test".to_string(),
//         object_store,
//         block_cache,
//         meta_cache_capacity: 1024,
//     };
//     let sstable_store = Arc::new(SstableStore::new(sstable_store_options));
//     let version_manager_options = VersionManagerOptions {
//         levels_options: vec![
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::Overlap,
//                 compression_algorithm: CompressionAlgorithm::None,
//             },
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::NonOverlap,
//                 compression_algorithm: CompressionAlgorithm::None,
//             },
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::NonOverlap,
//                 compression_algorithm: CompressionAlgorithm::None,
//             },
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::NonOverlap,
//                 compression_algorithm: CompressionAlgorithm::Lz4,
//             },
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::NonOverlap,
//                 compression_algorithm: CompressionAlgorithm::Lz4,
//             },
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::NonOverlap,
//                 compression_algorithm: CompressionAlgorithm::Lz4,
//             },
//             LevelOptions {
//                 compaction_strategy: LevelCompactionStrategy::NonOverlap,
//                 compression_algorithm: CompressionAlgorithm::Lz4,
//             },
//         ],
//         levels: vec![vec![]; 7],
//         sstable_store: sstable_store.clone(),
//     };

//     let version_manager = VersionManager::new(version_manager_options);

//     let lsm_tree_options = ObjectStoreLsmTreeOptions {
//         sstable_store: sstable_store.clone(),
//         write_buffer_capacity: 64 * 1024,
//         version_manager: version_manager.clone(),
//     };
//     let lsm_tree = ObjectStoreLsmTree::new(lsm_tree_options);

//     let uploader_options = SstableUploaderOptions {
//         lsm_tree: lsm_tree.clone(),
//         sstable_store,
//         version_manager,
//         sstable_capacity: 16 * 1024,
//         block_capacity: 1024,
//         restart_interval: 2,
//         bloom_false_positive: 0.1,
//         compression_algorithm: CompressionAlgorithm::None,
//         poll_interval: Duration::from_millis(100),
//     };
//     let uploader = SstableUploader::new(uploader_options);
//     tokio::spawn(async move { uploader.run().await });
//     lsm_tree
// }

// fn key(i: u64) -> Bytes {
//     Bytes::from(format!("k{:064}", i))
// }

// fn value(i: u64) -> Bytes {
//     Bytes::from(format!("v{:064}", i))
// }
