SHELL := /bin/bash
.PHONY: proto

rust_cargo_sort_check:
	cargo sort -c -w

fmt:
	cargo fmt --all

fmt_check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets -- -D warnings

clippy_locked:
	cargo clippy --all-targets --locked -- -D warnings

test:
	cargo nextest run

proto:
	cd proto/src/proto && prototool format -w && buf lint

proto_check:
	cd proto/src/proto && prototool format -d && buf lint 

update_ci:
	cd .github/template && ./generate.sh