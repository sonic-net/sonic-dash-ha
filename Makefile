.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS += -e

#
# Top level targets
#
all: format lint build test
release: format lint build-release test-release

#
# Install dependencies
#
install-deps:
	sudo apt install -y protobuf-compiler libprotobuf-dev

#
# Format tasks
#
format:
	cargo fmt -- --emit files

#
# Lint tasks
#
pre-commit:
	pre-commit run --all-files

lint:
	cargo clippy --all-targets --all-features

lint-fix:
	cargo clippy --all-targets --all-features --fix --allow-dirty

#
# Debug build targets
#
build:
	cargo build --all

test:
	cargo test --all

clean:
	cargo clean

#
# Release build targets
#
build-release:
	cargo build --release --all

test-release:
	cargo test --release --all