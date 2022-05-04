use core::panic;
use std::time::Instant;

use bytesize::ByteSize;
use clap::Parser;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
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

    #[clap(long, default_value = "10")]
    groups: usize,
    #[clap(long, default_value = "1048576")] // 1 << 20, 1_048_576
    entries: usize,
    #[clap(long, default_value = "10")]
    batch_size: usize,
    #[clap(long, default_value = "64")]
    data_size: usize,
    #[clap(long, default_value = "16")]
    context_size: usize,

    #[clap(long, default_value = "0")]
    seed: u64,
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
        let metadata = tokio::fs::metadata(entry.path()).await.unwrap();
        size += metadata.len() as usize;
    }
    size
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("Open raft log store...");

    let raft_log_store = build_raft_log_store(&args).await;
    for group in 1..=args.groups {
        raft_log_store.add_group(group as u64).await.unwrap();
    }

    println!("Prepare dataset...");

    let dataset = build_dataset(&args);

    println!("Start benching...");

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
