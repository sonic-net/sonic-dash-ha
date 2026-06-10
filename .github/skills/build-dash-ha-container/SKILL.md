---
name: build-dash-ha-container
description: 'Create the sonic-dash-ha Docker build container from scratch and verify the repo compiles. Use when: setting up a new build container, bootstrapping the dash-ha dev environment, "create the build container", "set up sonic-dash-ha container", installing build dependencies, building libswsscommon.'
argument-hint: 'Optional: container name, repo path, or base image to override defaults'
---

# Build the sonic-dash-ha Container

## When to Use
- Setting up a fresh local build environment for sonic-dash-ha
- The `sonic-dash-ha` build container does not exist yet (check with `docker ps -a`)
- Onboarding a new developer who needs a reproducible build container

This skill **creates** the build container. To compile/test in an existing
container, use the `build-test-dash-ha` skill instead.

## Prerequisites
- Docker installed and running (`docker --version`)
- Network access to pull `ubuntu:22.04`, clone `sonic-swss-common`, and fetch crates

## Default Configuration

| Parameter | Default Value |
|-----------|--------------|
| Container name | `sonic-dash-ha` |
| Base image | `ubuntu:22.04` |
| Host repo path | `<path-to-sonic-dash-ha>` |
| Container repo path | `/sonic-dash-ha` (bind-mounted from host) |
| swss-common path | `/opt/sonic-swss-common` |

The host repo path is a placeholder — substitute the actual path to the user's
local sonic-dash-ha checkout. If the user provides a different container name or
base image, use those instead.

## Procedure

### Step 1: Create the container

Bind-mount the host repo so builds operate on the working tree directly.

```bash
docker run -d --name sonic-dash-ha --hostname sonic-dash-ha \
  -v <path-to-sonic-dash-ha>:/sonic-dash-ha \
  -w /sonic-dash-ha ubuntu:22.04 sleep infinity
docker ps --filter name=sonic-dash-ha --format '{{.Names}}\t{{.Status}}'
```

If a container with the same name already exists, stop and tell the user.
Do not delete it without confirmation.

### Step 2: Install system build dependencies

This includes the Rust/protobuf toolchain, the full `sonic-swss-common` build
dependency chain, **and `libclang-dev`/`clang`** (required by `bindgen` in the
`swss-common` crate — easy to miss).

```bash
docker exec -e DEBIAN_FRONTEND=noninteractive sonic-dash-ha bash -c "apt-get update && apt-get install -y --no-install-recommends \
  build-essential make protobuf-compiler libprotobuf-dev curl git ca-certificates \
  pkg-config libssl-dev autoconf automake libtool libhiredis-dev \
  libnl-3-dev libnl-genl-3-dev libnl-route-3-dev libnl-nf-3-dev swig4.0 \
  libboost-serialization-dev libboost-dev uuid-dev libzmq3-dev libgtest-dev libgmock-dev \
  cmake nlohmann-json3-dev python3 python3-dev python-is-python3 cython3 \
  libclang-dev clang"
```

### Step 3: Install the Rust toolchain

```bash
docker exec sonic-dash-ha bash -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable"
docker exec sonic-dash-ha /root/.cargo/bin/rustc --version
```

### Step 4: Build libswsscommon (with the redisapi.h patch)

Clone `sonic-swss-common`, apply the README patch that redirects the hard-coded
Lua script path, configure **with YANG modules disabled** (avoids the
`sonic_yang`/libyang dependency, which isn't needed for the FFI bridge), then build.

```bash
docker exec sonic-dash-ha bash -c "cd /opt && git clone --depth 1 https://github.com/sonic-net/sonic-swss-common && \
  sed -i 's#return readTextFile(\"/usr/share/swss/\" + path);#return readTextFile(\"/opt/sonic-swss-common/common/\" + path);#' /opt/sonic-swss-common/common/redisapi.h"

docker exec sonic-dash-ha bash -c "cd /opt/sonic-swss-common && ./autogen.sh && \
  ./configure --enable-yangmodules=no && make -j \$(nproc)"
```

Verify the shared library was produced:

```bash
docker exec sonic-dash-ha bash -c "ls -l /opt/sonic-swss-common/common/.libs/libswsscommon.so"
```

### Step 5: Persist build environment variables

Write the env vars to a profile script and source it from `.bashrc` so every
`docker exec bash -l` session has them.

```bash
docker exec sonic-dash-ha bash -c 'printf "export PATH=\"/root/.cargo/bin:\$PATH\"\nexport SWSS_COMMON_REPO=\"/opt/sonic-swss-common\"\nexport LD_LIBRARY_PATH=\"/opt/sonic-swss-common/common/.libs\"\n" > /etc/profile.d/dash-ha.sh; \
  grep -q dash-ha.sh /root/.bashrc || echo "source /etc/profile.d/dash-ha.sh" >> /root/.bashrc'
```

### Step 6: Verify the repo builds

```bash
docker exec sonic-dash-ha bash -lc "cd /sonic-dash-ha && cargo build --workspace 2>&1 | tail -n 30"
```

A successful run ends with `Finished \`dev\` profile ... target(s)` and exit code 0,
compiling all crates including `hamgrd`, `swbusd`, and `swss-common-bridge`.

## Gotchas

- **libclang**: The `swss-common` crate uses `bindgen`, which needs `libclang-dev`.
  Without it the build fails with "Unable to find libclang". Not mentioned in the README.
- **YANG modules**: The default `sonic-swss-common` build enables YANG modules and
  requires the `sonic_yang` Python module + libyang. Use `--enable-yangmodules=no`
  to skip that dependency chain — the FFI bridge does not need it.
- **Python required by configure**: `sonic-swss-common`'s `configure` aborts without
  a Python interpreter; install `python3`/`python3-dev` before configuring.
- **redisapi.h patch**: The hard-coded `/usr/share/swss/` path must be patched to the
  clone location, or runtime Lua script loading breaks. See the README for details.
- **Bind mount vs. docker cp**: This skill bind-mounts the host repo so edits are
  immediately visible. The `build-test-dash-ha` skill instead copies a fresh tree in;
  pick whichever workflow the user expects.
- **Base image**: `ubuntu:22.04` (jammy) provides compatible `protoc`, boost, and
  libclang-14 versions. Newer bases may pull incompatible toolchain versions.
