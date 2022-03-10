SHELL := /bin/bash
.PHONY: all
all: rust_test_with_coverage

# Note: "--skip-clean" must be used along with "CARGO_TARGET_DIR=..."
# See also https://github.com/xd009642/tarpaulin/issues/777
rust_test_with_coverage:
	RUSTFLAGS=-Dwarnings CARGO_TARGET_DIR=target/tarpaulin cargo tarpaulin --workspace \
	--exclude proto --exclude-files tests/* --out Xml --force-clean --run-types Doctests Tests \
	-- --report-time -Z unstable-options