use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use parking_lot::RwLock;
use runkv_common::sharded_hash_map::ShardedHashMap;

const CONCURRENCY: u64 = 1024;
const SLEEP: Duration = Duration::from_micros(200);

fn sharded_hash_map_concurrent_put_get(shards: u16) {
    let map = ShardedHashMap::new(shards);

    let job = |map: ShardedHashMap<u64, u64>, i: u64, total: u64| {
        {
            assert_eq!(map.insert(i, i), None);
        }
        {
            let read = map.read(&i);
            assert_eq!(read.get(), Some(&i));
            std::thread::sleep(SLEEP);
            drop(read);
        }
        {
            let mut write = map.write(&i);
            assert_eq!(write.get(), Some(&i));
            *write.get_mut().unwrap() += total;
            assert_eq!(write.get(), Some(&(i + total)));
            std::thread::sleep(SLEEP);
            drop(write);
        }
        {
            let read = map.read(&i);
            assert_eq!(read.get(), Some(&(i + total)));
            std::thread::sleep(SLEEP);
            drop(read);
        }
    };

    let handles = (0..CONCURRENCY)
        .into_iter()
        .map(|i| {
            let map_clone = map.clone();
            std::thread::spawn(move || job(map_clone, i, CONCURRENCY))
        })
        .collect_vec();

    for handle in handles {
        handle.join().unwrap();
    }
}

fn hash_map_concurrent_put_get() {
    let map = Arc::new(RwLock::new(HashMap::default()));

    let job = |map: Arc<RwLock<HashMap<u64, u64>>>, i: u64, total: u64| {
        {
            assert_eq!(map.write().insert(i, i), None);
        }
        {
            let read = map.read();
            assert_eq!(read.get(&i), Some(&i));
            std::thread::sleep(SLEEP);
            drop(read);
        }
        {
            let mut write = map.write();
            assert_eq!(write.get(&i), Some(&i));
            *write.get_mut(&i).unwrap() += total;
            assert_eq!(write.get(&i), Some(&(i + total)));
            std::thread::sleep(SLEEP);
            drop(write);
        }
        {
            let read = map.read();
            assert_eq!(read.get(&i), Some(&(i + total)));
            std::thread::sleep(SLEEP);
            drop(read);
        }
    };

    let handles = (0..CONCURRENCY)
        .into_iter()
        .map(|i| {
            let map_clone = map.clone();
            std::thread::spawn(move || job(map_clone, i, CONCURRENCY))
        })
        .collect_vec();

    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench_hash_map_concurrent_put_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("10 samples");
    group.sample_size(10);

    group.bench_function("hash map curruent put get", |b| {
        b.iter(hash_map_concurrent_put_get)
    });

    group.bench_function("sharded hash map curruent put get - 1 shard(s)", |b| {
        b.iter(|| sharded_hash_map_concurrent_put_get(1))
    });

    group.bench_function("sharded hash map curruent put get - 16 shard(s)", |b| {
        b.iter(|| sharded_hash_map_concurrent_put_get(16))
    });

    group.bench_function("sharded hash map curruent put get - 64 shard(s)", |b| {
        b.iter(|| sharded_hash_map_concurrent_put_get(64))
    });

    group.bench_function("sharded hash map curruent put get - 256 shard(s)", |b| {
        b.iter(|| sharded_hash_map_concurrent_put_get(256))
    });

    group.bench_function("sharded hash map curruent put get - 1024 shard(s)", |b| {
        b.iter(|| sharded_hash_map_concurrent_put_get(1024))
    });

    group.finish();
}

criterion_group!(benches, bench_hash_map_concurrent_put_get);
criterion_main!(benches);
