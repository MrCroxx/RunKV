use std::sync::Arc;

use bytes::BufMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use runkv_common::coding::CompressionAlgorithm;
use runkv_storage::components::{Block, BlockBuilder, BlockBuilderOptions};
use runkv_storage::iterator::{BlockIterator, Seek};

const TABLES_PER_SSTABLE: u32 = 10;
const KEYS_PER_TABLE: u64 = 100;
const RESTART_INTERVAL: usize = 16;
const BLOCK_CAPACITY: usize = TABLES_PER_SSTABLE as usize * KEYS_PER_TABLE as usize * 64;

async fn block_iter_next(block: Arc<Block>) {
    let mut iter = BlockIterator::new(block);
    iter.seek(Seek::First).unwrap();
    while iter.is_valid() {
        iter.next().unwrap();
    }
}

async fn block_iter_prev(block: Arc<Block>) {
    let mut iter = BlockIterator::new(block);
    iter.seek(Seek::Last).unwrap();
    while iter.is_valid() {
        iter.prev().unwrap();
    }
}

fn bench_block_iter(c: &mut Criterion) {
    let block = Arc::new(build_block(TABLES_PER_SSTABLE, KEYS_PER_TABLE));

    println!("block size: {}", block.len());

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "block - iter next - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &block,
        |b, block| {
            b.iter(|| block_iter_next(block.clone()));
        },
    );

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "block - iter prev - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &block,
        |b, block| {
            b.iter(|| block_iter_prev(block.clone()));
        },
    );

    let mut iter = BlockIterator::new(block);
    iter.seek(Seek::First).unwrap();
    for t in 1..=TABLES_PER_SSTABLE {
        for i in 1..=KEYS_PER_TABLE {
            assert_eq!(iter.key(), key(t, i).to_vec());
            assert_eq!(iter.value(), value(i).to_vec());
            iter.next().unwrap();
        }
    }
    assert!(!iter.is_valid());
}

criterion_group!(benches, bench_block_iter);
criterion_main!(benches);

fn build_block(t: u32, i: u64) -> Block {
    let options = BlockBuilderOptions {
        capacity: BLOCK_CAPACITY,
        compression_algorithm: CompressionAlgorithm::None,
        restart_interval: RESTART_INTERVAL,
    };
    let mut builder = BlockBuilder::new(options);
    for tt in 1..=t {
        for ii in 1..=i {
            builder.add(&key(tt, ii), &value(ii));
        }
    }
    let data = builder.build();
    Block::decode(data).unwrap()
}

fn key(t: u32, i: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u8(b't');
    buf.put_u32(t);
    buf.put_u64(i);
    buf
}

fn value(i: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(i);
    buf
}
