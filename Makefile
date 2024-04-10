.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS += -e

#
# Release build targets
#
all: build

build:
	cargo build --release --all

#
# Debug build targets
#
dbg: build-debug

build-debug:
	cargo build --all

#
# Install dependencies
#
install-deps:
	sudo apt install -y protobuf-compiler libprotobuf-dev