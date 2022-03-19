SHELL := /bin/bash
.PHONY: proto

fmt:
	cargo sort -w && cargo fmt --all && cargo clippy --all-targets

fmt_check:
	cargo sort -c -w && cargo fmt --all -- --check && cargo clippy --all-targets --locked -- -D warnings

test:
	cargo nextest run

proto:
	cd proto/src/proto && prototool format -w && buf lint

proto_check:
	cd proto/src/proto && prototool format -d && buf lint 

update_ci:
	cd .github/template && ./generate.sh