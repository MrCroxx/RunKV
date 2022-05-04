use core::panic;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use runkv_storage::raft_log_store::entry::RaftLogBatchBuilder;
use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
use runkv_storage::raft_log_store::RaftLogStore;

#[derive(Debug, Clone)]
struct Args {
    path: String,
    log_file_capacity: usize,
    block_cache_capacity: usize,

    groups: usize,
    entries: usize,
    batch_size: usize,
    data_size: usize,
    context_size: usize,

    seed: u64,
}

impl Args {
    pub fn env_or_default() -> Self {
        let path = std::env::var("BENCH_RAFT_LOG_STORE_PATH")
            .unwrap_or_else(|_| ".run/data/bench_raft_log_store_data".to_string());
        let log_file_capacity = match std::env::var("BENCH_RAFT_LOG_STORE_LOG_FILE_CAPACITY") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 4 << 20,
        };
        let block_cache_capacity = match std::env::var("BENCH_RAFT_LOG_STORE_BLOCK_CACHE_CAPACITY")
        {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 64 << 20,
        };
        let groups = match std::env::var("BENCH_RAFT_LOG_STORE_GROUPS") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 10,
        };
        let entries = match std::env::var("BENCH_RAFT_LOG_STORE_ENTRIES") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 1 << 20,
        };
        let batch_size = match std::env::var("BENCH_RAFT_LOG_STORE_BATCH_SIZE") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 10,
        };
        let data_size = match std::env::var("BENCH_RAFT_LOG_STORE_DATA_SIZE") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 64,
        };
        let context_size = match std::env::var("BENCH_RAFT_LOG_STORE_CONTEXT_SIZE") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 16,
        };
        let seed = match std::env::var("BENCH_RAFT_LOG_STORE_SEED") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 0,
        };
        Self {
            path,
            log_file_capacity,
            block_cache_capacity,

            groups,
            entries,
            batch_size,
            data_size,
            context_size,

            seed,
        }
    }
}

async fn build_raft_log_store(args: &Args) -> RaftLogStore {
    if tokio::fs::metadata(&args.path).await.is_ok() {
        panic!("path for benching raft log store already exists");
    }
    tokio::fs::create_dir_all(&args.path).await.unwrap();

    let raft_log_store_options = RaftLogStoreOptions {
        log_dir_path: args.path.to_owned(),
        log_file_capacity: args.log_file_capacity,
        block_cache_capacity: args.block_cache_capacity,
    };
    RaftLogStore::open(raft_log_store_options).await.unwrap()
}

/// ```plain
/// [
///   [
///     (group, term, index, ctx, data),
///     ...
///   ],
///   ...
/// ]
/// ```
#[allow(clippy::type_complexity)]
fn build_dataset(args: &Args) -> Vec<Vec<(u64, u64, u64, Vec<u8>, Vec<u8>)>> {
    const TERM: u64 = 1;
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    let mut dataset = Vec::with_capacity(args.entries / args.batch_size + 1);

    for group in 1..=args.groups {
        for start in (1..=args.entries).step_by(args.batch_size) {
            let mut batch = Vec::with_capacity(args.groups * args.batch_size);
            for index in start..=start + args.batch_size {
                let mut context: Vec<u8> = vec![0; args.context_size];
                let mut data: Vec<u8> = vec![0; args.data_size];
                rng.fill(&mut context[..]);
                rng.fill(&mut data[..]);
                batch.push((group as u64, TERM, index as u64, context, data));
            }
            dataset.push(batch);
        }
    }
    dataset
}

async fn dir_size(path: &str) -> usize {
    let mut size = 0;
    let mut dir = tokio::fs::read_dir(path).await.unwrap();
    while let Some(entry) = dir.next_entry().await.unwrap() {
        println!("log file: {:?}", entry.path());
        let metadata = tokio::fs::metadata(entry.path()).await.unwrap();
        size += metadata.len() as usize;
    }
    size
}

fn bench_raft_log_store(c: &mut Criterion) {
    let args = Args::env_or_default();

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("id", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let args_clone = args.clone();
            async move {
                black_box(
                    async move {
                        let mut total = Duration::from_nanos(0);
                        for _ in 0..iters {
                            let dataset = build_dataset(&args_clone);
                            let raft_log_store = build_raft_log_store(&args_clone).await;
                            for group in 1..=args_clone.groups {
                                raft_log_store.add_group(group as u64).await.unwrap();
                            }

                            let start = Instant::now();

                            for data in dataset {
                                let mut builder = RaftLogBatchBuilder::default();
                                for (group, term, index, ctx, data) in data {
                                    builder.add(group, term, index, &ctx, &data);
                                }
                                let batches = builder.build();
                                for batch in batches {
                                    raft_log_store.append(batch).await.unwrap();
                                }
                            }

                            total += start.elapsed();
                        }
                        total
                    }
                    .await,
                )
            }
        })
    });

    rt.block_on(async {
        let total_size = dir_size(&args.path).await;
        println!("total size: {}", total_size);
        tokio::fs::remove_dir_all(&args.path).await.unwrap();
    })
}

criterion_group!(benches, bench_raft_log_store);
criterion_main!(benches);
