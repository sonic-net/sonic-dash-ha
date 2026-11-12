# Native AMD64 source builds with Bazel

The Bazel path builds all 14 Cargo workspace crates on Debian Trixie. The
installed output follows `debian/install`: `hamgrd`, `swbusd`, `swbus-cli`, and
`wait_for_loopback.py`. The `container` program is a separate build utility.
The output is a runtime TAR, a matching debug TAR, and source-package metadata.
Docker integration is a separate consumer; these targets do not produce DEBs.

## Build and test

Use Bazel 8.5.1 on native Debian Trixie AMD64. The target and host platform
settings both select Trixie for the native dependency graph. GCC, Rust 1.90.0,
Protobuf, Common's native/Rust libraries, and the runtime test tools are declared
dependencies.

```sh
bazel build --config=release //:dash_ha_pkg //:dash_ha_pkg.debug_symbols //:runtime_packages.json
bazel test --config=release //crates/... //bazel:installed_package_test
bazel build --config=release //crates/container:container
```

The normal registry configuration is maintained SONiC `main` plus BCR.
Run the same dev and release checks as CI:

```sh
python3 bazel/ci.py
```

The driver requires a clean checkout and uses maintained SONiC `main` plus BCR.
It retains resolved inputs, the configured no-DEB graph, matching runtime/debug
packages, Rustdoc output, and test XML/logs separately under `artifacts/dev/`
and `artifacts/release/`, before switching configuration. The two original
Rustdoc examples are compiled by `//crates/swbus-actor:doc_test`.
This preserves the dev/release selection in `make ci-test`. Cargo metadata
reports no optional features in the current workspace members, so its
`--all-features` option adds no workspace feature variants.
The shared infrastructure owns the immutable APT source configuration and
production lock; this consumer imports its existing `trixie` collection.
The root registry configuration is retained as `root.bazelrc` for artifact upload.
Generated `MODULE.bazel.lock` files are evidence and remain untracked.
`Cargo.lock` is the canonical Cargo lock. Repeated local runs reuse Bazel's
output tree. Optional shared local caches belong in ignored `.bazelrc.user`:

```text
common --repository_cache=/absolute/path/to/bazel/repository-cache
build --disk_cache=/absolute/path/to/bazel/disk-cache
```

## Source ownership and compatibility

Each crate has a maintained `BUILD.bazel` beside its Rust code. Shared third
party crates come from `sonic-rust-deps`; reusable Common Rust libraries and
its testing helpers come from `sonic-swss-common`. Cargo and Bazel select the
same reviewed Common source. Common's default and async variants retain
matching testing helpers and shared Serde/Tokio types.

Both original `build.rs` generators remain intact. The SWBus generator uses
its existing Tonic protocol. The HA API generator consumes the API owner's
`ha_schema_sources` at original revision
`18a29c1c8a21aeb22a4893c786564acaf8212156`. That schema uses
`ha_scope_id = 1` and `vdpu_id = 2`; the current API's swapped field numbers
would change HA's wire format. Only the file layout adapter belongs here;
the schema bytes and their source archive remain API-owned.

The procedural macro only emits Common references into the caller's code.
Its Common dependencies are therefore development dependencies for its
integration test, rather than libraries loaded into the compiler process.
This also keeps the native Cargo build valid.

## Validation and routine changes

The original crate and integration tests run with their declared JSON
fixtures and Common's original private Redis lifetime/cleanup behavior.
Common's Lua files and the Redis/timeout/kill programs come from declared
runtime inputs. Installed-package validation checks the exact native install
inventory, TAR headers, AMD64 ELF dependency closure, matching build IDs and
debug links, and source lines in GDB. A negative GDB control omits the debug
TAR. A private loopback `swbusd`/`swbus-cli` exchange exercises the installed
programs. This does not prove ASIC HA operation or Docker integration.

For a normal Rust source addition, update the owning crate; recursive source
globs include its module files. Add each dependency to that crate's `deps`,
and extend the shared crate owner when a reusable third party is missing.
When adding a Cargo workspace member, add its component `BUILD.bazel`; CI
compares the two inventories. Add integration test targets and fixture labels
beside their source, preserving each original test entry point. An installed
program change must update `debian/install`, the root TAR target and installed
acceptance together. CI checks the source inventory and retains every test
configuration through Bazel's build events. The three original Common
async/testing compatibility targets run separately through the test-only
`//bazel:common_test_runner` launcher, which delegates to Common's existing
declared-runtime runner. This keeps the original owner tests and shared
Rust types intact. CI retains built packages before running tests, so a
failing test still uploads its exact source inputs and produced TARs.
