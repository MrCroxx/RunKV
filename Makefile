SHELL := /bin/bash
.PHONY: all
all: rust

rust: rust_check_all rust_test_with_coverage rust_cargo_sort_check

rust_check_all: rust_cargo_sort_check rust_fmt_check rust_clippy_check rust_clippy_check_locked

rust_cargo_sort_check:
	cargo sort -c -w

rust_fmt_check:
	cargo fmt --all -- --check

rust_clippy_check:
	cd rust && cargo clippy --all-targets -- -D warnings

rust_clippy_check_locked:
	cd rust && cargo clippy --all-targets --locked -- -D warnings

# Note: "--skip-clean" must be used along with "CARGO_TARGET_DIR=..."
# See also https://github.com/xd009642/tarpaulin/issues/777
rust_test_with_coverage:
	RUSTFLAGS=-Dwarnings CARGO_TARGET_DIR=target/tarpaulin cargo tarpaulin --workspace \
	--exclude proto --exclude-files tests/* --out Xml --force-clean --run-types Doctests Tests \
	-- --report-time -Z unstable-options
