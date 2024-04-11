.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS += -e

#
# Debug build targets
#
all: build test

build:
	cargo build --all

test:
	cargo test --all

#
# Release build targets
#
release: build-release test-release

build-release:
	cargo build --release --all

test-release:
	cargo test --release --all

#
# Install dependencies
#
install-deps:
	sudo apt install -y protobuf-compiler libprotobuf-dev