SHELL := /bin/bash
.PHONY: proto

fmt:
	cargo sort -w && cargo fmt --all && cargo clippy --all-targets --all-features && cargo clippy --all-targets

fmt_check:
	cargo sort -c -w && cargo fmt --all -- --check && cargo clippy --all-targets --all-features --locked -- -D warnings && cargo clippy --all-targets --locked -- -D warnings

clean:
	cargo clean

check:
	cargo check --tests

test:
	cargo nextest run --features deadlock

proto:
	cd proto/src/proto && prototool format -w && buf lint

proto_check:
	cd proto/src/proto && prototool format -d && buf lint 

update_ci:
	cd .github/template && ./generate.sh

bench_kv:
	RUNKV_METRICS=true RUST_BACKTRACE=1 cargo run --release --package runkv-bench --bin bench_kv

bench_runkv_1:
	./run k || true && rm -rf .run/tmp/bench-kv && ./run d && cargo build --release --package runkv-bench --bin bench_kv --features "verbose-release-log" && RUNKV_METRICS=true RUST_BACKTRACE=1 RUST_LOG=debug ./target/release/bench_kv --persist none --wheels 6 --loop 600 --groups 12 --s3-uri minio://minioadmin:minioadmin@127.0.0.1:9000/test --key-size 512 --value-size 512 --concurrency 10 --mc 0.1

bench_runkv_1:
	./run k || true && rm -rf .run/tmp/bench-kv && ./run d && cargo build --release --package runkv-bench --bin bench_kv --features "verbose-release-log" && RUNKV_METRICS=true RUST_BACKTRACE=1 RUST_LOG=debug ./target/release/bench_kv --persist none --wheels 6 --loop 600 --groups 12 --s3-uri minio://minioadmin:minioadmin@127.0.0.1:9000/test --key-size 512 --value-size 512 --concurrency 8 --mc 0.05

bench_rocksdb_cloud_like:
	./run k || true && rm -rf .run/tmp/bench-kv && ./run d && cargo build --release --package runkv-bench --bin bench_kv --features "verbose-release-log" && RUNKV_METRICS=true RUST_BACKTRACE=1 RUST_LOG=debug ./target/release/bench_kv --persist none --wheels 6 --loop 600 --groups 12 --s3-uri minio://minioadmin:minioadmin@127.0.0.1:9000/test --key-size 512 --value-size 512 --concurrency 6 --mc 0.06

bench_tikv_like:
	./run k || true && rm -rf .run/tmp/bench-kv && ./run d && cargo build --release --package runkv-bench --bin bench_kv --features "verbose-release-log" && RUNKV_METRICS=true RUST_BACKTRACE=1 RUST_LOG=debug ./target/release/bench_kv --persist none --wheels 6 --loop 600 --groups 12 --s3-uri minio://minioadmin:minioadmin@127.0.0.1:9000/test --key-size 512 --value-size 512 --concurrency 10 --mc 0.15
