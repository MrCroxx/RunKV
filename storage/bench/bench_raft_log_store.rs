use core::panic;
use std::time::Instant;

use bytesize::ByteSize;
use clap::Parser;
use futures::future;
use itertools::Itertools;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use runkv_common::prometheus::DefaultPrometheusExporter;
use runkv_storage::raft_log_store::entry::RaftLogBatchBuilder;
use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
use runkv_storage::raft_log_store::RaftLogStore;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(long, default_value = "4194304")] // 4 MiB
    log_file_capacity: usize,
    #[clap(long, default_value = "67108864")] // 64 MiB
    block_cache_capacity: usize,

    #[clap(long, default_value = "100")]
    groups: usize,
    #[clap(long, default_value = "10000")]
    entries: usize,
    #[clap(long, default_value = "10")]
    batch_size: usize,
    #[clap(long, default_value = "64")]
    data_size: usize,
    #[clap(long, default_value = "16")]
    context_size: usize,

    #[clap(long, default_value = "0")]
    seed: u64,

    #[clap(long)]
    metrics: bool,
    #[clap(long, default_value = "127.0.0.1")]
    prometheus_host: String,
    #[clap(long, default_value = "9898")]
    prometheus_port: u16,
}

async fn build_raft_log_store(args: &Args) -> RaftLogStore {
    if tokio::fs::metadata(&args.path).await.is_ok() {
        panic!("path for benching raft log store already exists");
    }
    tokio::fs::create_dir_all(&args.path).await.unwrap();

    let raft_log_store_options = RaftLogStoreOptions {
        node: 0,
        log_dir_path: args.path.to_owned(),
        log_file_capacity: args.log_file_capacity,
        block_cache_capacity: args.block_cache_capacity,
    };
    RaftLogStore::open(raft_log_store_options).await.unwrap()
}

/// ```plain
/// [
///   // batches for group
///   [
///     [
///       (group, term, index, ctx, data),
///       ...
///     ],
///     ...
///   ],
///   ...
/// ]
/// ```
#[allow(clippy::type_complexity)]
fn build_dataset(args: &Args) -> Vec<Vec<Vec<(u64, u64, u64, Vec<u8>, Vec<u8>)>>> {
    const TERM: u64 = 1;
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    let mut dataset = Vec::with_capacity(args.groups);

    for group in 1..=args.groups {
        let mut group_dataset = Vec::with_capacity(args.entries / args.batch_size + 1);
        for start in (1..=args.entries).step_by(args.batch_size) {
            let mut batch = Vec::with_capacity(args.batch_size);
            for index in start..start + args.batch_size {
                if index > args.entries {
                    break;
                }
                let mut context: Vec<u8> = vec![0; args.context_size];
                let mut data: Vec<u8> = vec![0; args.data_size];
                rng.fill(&mut context[..]);
                rng.fill(&mut data[..]);
                batch.push((group as u64, TERM, index as u64, context, data));
            }
            group_dataset.push(batch);
        }
        dataset.push(group_dataset);
    }
    dataset
}

async fn dir_size(path: &str) -> usize {
    let mut size = 0;
    let mut dir = tokio::fs::read_dir(path).await.unwrap();
    while let Some(entry) = dir.next_entry().await.unwrap() {
        let metadata = tokio::fs::metadata(entry.path()).await.unwrap();
        size += metadata.len() as usize;
    }
    size
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("{:#?}", args);

    if args.metrics {
        let addr = format!("{}:{}", args.prometheus_host, args.prometheus_port)
            .parse()
            .unwrap();
        DefaultPrometheusExporter::init(addr);
    }

    println!("Open raft log store...");

    let raft_log_store = build_raft_log_store(&args).await;
    for group in 1..=args.groups {
        raft_log_store.add_group(group as u64).await.unwrap();
    }

    println!("Prepare dataset...");

    let dataset = build_dataset(&args);

    let futures = dataset
        .into_iter()
        .map(|batches| {
            let raft_log_store_clone = raft_log_store.clone();
            async move {
                for batch in batches {
                    let mut builder = RaftLogBatchBuilder::default();
                    for (group, term, index, ctx, data) in batch {
                        builder.add(group, term, index, &ctx, &data);
                    }
                    let log_batches = builder.build();
                    raft_log_store_clone.append(log_batches).await.unwrap();
                }
            }
        })
        .collect_vec();

    println!("Start benching...");

    let start = Instant::now();

    future::join_all(futures).await;

    let elapsed = start.elapsed();

    println!("Finish benching...");

    println!();

    println!("========== Bench Result ==========");

    println!();

    let total_size = dir_size(&args.path).await;
    println!(
        "Duration: {:.3}s",
        (elapsed.as_secs_f64() * 1000.0).round() / 1000.0
    );

    println!();

    println!(
        "Total Size: {}",
        ByteSize(total_size as u64).to_string_as(true)
    );

    println!();

    println!(
        "Bandwidth: {}/s",
        ByteSize((total_size as f64 / elapsed.as_secs_f64()).round() as u64).to_string_as(true)
    );

    println!();

    println!("========== Bench Result ==========");

    println!();

    println!("Clean temporary files...");

    tokio::fs::remove_dir_all(&args.path).await.unwrap();
}
