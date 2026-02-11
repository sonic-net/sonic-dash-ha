# Copilot Instructions for sonic-dash-ha

## Project Overview

sonic-dash-ha implements High Availability (HA) services for SONiC SmartSwitch DASH deployments. Written primarily in Rust, it provides the HA manager daemon (`hamgrd`), a service bus (`swbus`) for inter-service communication, and supporting libraries. These components coordinate failover, state synchronization, and health monitoring across DASH appliances.

## Architecture

```
sonic-dash-ha/
├── crates/                       # Rust workspace — all code lives here
│   ├── hamgrd/                   # HA Manager daemon — main HA orchestrator
│   ├── swbus-core/               # Service bus core library (message routing)
│   ├── swbusd/                   # Service bus daemon
│   ├── swbus-edge/               # Service bus edge client library
│   ├── swbus-cli/                # Service bus CLI tool
│   ├── swbus-proto/              # Service bus protobuf definitions
│   ├── swbus-config/             # Service bus configuration
│   ├── swbus-actor/              # Actor framework for swbus
│   ├── sonic-dash-api-proto/     # DASH API proto bindings for Rust
│   ├── sonic-common/             # Common SONiC utilities (logging, etc.)
│   ├── swss-common-bridge/       # FFI bridge to libswsscommon (C++ → Rust)
│   ├── swss-serde/               # Serialization for swss-common types
│   ├── sonicdb-derive/           # Derive macros for SONiC DB types
│   └── container/                # Container build and packaging
├── scripts/                      # Utility scripts
│   └── wait_for_loopback.py      # Wait for loopback interface readiness
├── test_utils/                   # Test utilities and helpers
├── debian/                       # Debian packaging
├── Makefile                      # Top-level build orchestration
└── .github/                      # CI workflows
```

### Key Concepts
- **Rust workspace**: All crates are managed as a single Cargo workspace
- **swbus**: A custom service bus for reliable inter-service messaging across SONiC components
- **hamgrd**: The HA manager daemon that coordinates failover decisions
- **swss-common bridge**: FFI bindings to `libswsscommon` for Redis DB access from Rust
- **Protobuf IPC**: Inter-service messages use protobuf encoding

## Language & Style

- **Primary language**: Rust (with C++ FFI for swss-common)
- **Secondary**: Python (scripts), C++ (FFI bridge code)
- **Rust conventions**:
  - Follow standard `rustfmt` formatting (enforced via `cargo fmt`)
  - Use `clippy` for linting (`cargo clippy --all-targets --all-features`)
  - Types: `PascalCase`
  - Functions/variables: `snake_case`
  - Constants: `UPPER_SNAKE_CASE`
  - Modules: `snake_case`
- **Error handling**: Use `Result<T, E>` types; prefer `anyhow` or custom error types over panics
- **Unsafe code**: Minimize `unsafe`; restrict to FFI boundaries in `swss-common-bridge`

## Build Instructions

```bash
# Install dependencies
sudo apt install -y protobuf-compiler libprotobuf-dev

# Install Rust (if not already)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Optional: Install libswsscommon for hamgrd (Linux only)
# See README for detailed swss-common setup instructions
export SWSS_COMMON_REPO="<path-to-sonic-swss-common>"
export LD_LIBRARY_PATH="$SWSS_COMMON_REPO/common/.libs"

# Build all (debug)
make build          # or: cargo build --all

# Build release
make release        # or: cargo build --all --release

# Format
make format         # or: cargo fmt

# Lint
make lint           # or: cargo clippy --all-targets --all-features

# Clean
cargo clean
```

## Testing

```bash
# Run all tests (debug)
make test           # or: cargo test --all

# Run all tests (release)
make test-release   # or: cargo test --all --release

# Run tests for a specific crate
cargo test -p hamgrd
cargo test -p swbus-core

# Test utilities are in test_utils/
```

## PR Guidelines

- **Signed-off-by**: REQUIRED on all commits (`git commit -s`)
- **CLA**: Sign the Linux Foundation EasyCLA
- **Single commit per PR**: Squash commits before merge
- **Formatting**: Run `cargo fmt` before submitting — CI enforces it
- **Linting**: Run `cargo clippy` — all warnings must be resolved
- **Pre-commit**: Run `make pre-commit` if pre-commit hooks are configured
- **Tests**: Add unit tests for new functionality; integration tests where applicable
- **Reference**: Link to SONiC DASH HA design documents in PR description

## Dependencies

- **sonic-swss-common**: C++ library for Redis DB access (linked via FFI bridge)
- **sonic-dash-api**: Protobuf definitions for DASH APP DB entries
- **protobuf / prost**: Protobuf compilation and runtime
- **tokio**: Async runtime for network services
- **tonic**: gRPC framework (for swbus communication)
- **Redis**: Backend database for SONiC state

## Gotchas

- **swss-common setup**: Building `hamgrd` requires a local build of `sonic-swss-common` with patched header paths — see README for the exact patch
- **LD_LIBRARY_PATH**: Must be set correctly when running tests that use `swss-common-bridge`
- **Cross-platform**: Some crates (`swss-common-bridge`, `hamgrd`) are Linux-only; pure Rust crates may work on other platforms
- **Protobuf version**: Ensure `protoc` version matches what `prost-build` expects
- **Cargo workspace**: Adding a new crate requires updating the workspace `Cargo.toml`
- **FFI safety**: Changes to `swss-common-bridge` must be carefully reviewed for memory safety
- **Feature flags**: Some crates use feature flags to conditionally compile swss-common integration
- **CI pipelines**: PRs are validated via GitHub Actions workflows in `.github/`
