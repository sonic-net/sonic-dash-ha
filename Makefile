.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS += -e

all: build

build:
	@echo "Building the project..."