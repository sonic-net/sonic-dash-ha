.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS += -e

#
# Release build targets
#
all: build

build:
	cargo build --release

#
# Debug build targets
#
dbg: build-debug

build-debug:
	cargo build --debug
