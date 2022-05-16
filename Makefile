SHELL := /bin/bash
.PHONY: proto

fmt:
	cargo sort -w && cargo fmt --all && cargo clippy --all-targets

fmt_check:
	cargo sort -c -w && cargo fmt --all -- --check && cargo clippy --all-targets --locked -- -D warnings

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