---
name: build-test-dash-ha
description: 'Copy dash-ha repo into build container and run compilation checks and unit tests. Use when: compile and test in sonic-dash-ha repo.'
argument-hint: 'Optional: specific make targets (e.g. ci-build, ci-test, ci-lint) or crate names to test'
---

# Build & Test in dash-ha Container

## When to Use
- After code changes in sonic-dash-ha, to verify compilation inside the build container
- To run unit tests in the same environment as CI
- To check for warnings, clippy lints, or formatting issues before pushing
- When the user says "build in container", "test in container", or "CI check"

## Prerequisites
- The `sonic-dash-ha` Docker build container must be running locally (check with `docker ps`)

## Default Configuration

| Parameter | Default Value |
|-----------|--------------|
| Build container | `sonic-dash-ha` |
| Host repo path | `<path-to-sonic-dash-ha>` |
| Container repo path | `/sonic-dash-ha` |

The host repo path is a placeholder — substitute the actual path to the user's
local sonic-dash-ha checkout.

## Procedure

### Step 1: Verify the build container is running

```bash
docker ps --filter name=sonic-dash-ha --format '{{.Names}}'
```

If the container is not running, stop and tell the user. Do not attempt to start it.

### Step 2: Copy repo into build container

```bash
docker exec sonic-dash-ha bash -c "rm -rf /sonic-dash-ha" && \
docker cp <path-to-sonic-dash-ha> sonic-dash-ha:/sonic-dash-ha
```

This removes the old source tree and copies a fresh one. The cargo build cache under `target/` is preserved for incremental builds.

### Step 3: Run compilation check

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && cargo build --workspace --all-features 2>&1"
```

If the user asks for a **CI-grade** build (strict, deny warnings), use:

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && RUSTFLAGS='--deny warnings' cargo build --workspace --all-features 2>&1"
```

Check the output for errors. If compilation fails, show the errors and stop — do not proceed to tests.

### Step 4: Run unit tests

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && cargo test --workspace --all-features 2>&1"
```

To test a **specific crate**:

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && cargo test -p <crate-name> 2>&1"
```

Report the test summary (number of passed/failed tests). If any tests fail, show the failure output.

## Additional Checks (on request)

These are only run when the user explicitly asks, or asks for "full CI" / "ci-all":

### Formatting check

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && cargo fmt --check --all 2>&1"
```

### Clippy lint check

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && cargo clippy --workspace --all-features --no-deps -- --deny 'clippy::all' 2>&1"
```

### Full CI pipeline

Runs all CI targets sequentially (format, build, doc, lint, test — both debug and release):

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && make ci-all 2>&1"
```

**Warning**: This is slow as it builds and tests in both debug and release modes with `cargo clean` between each stage.

## Gotchas

- **Build cache**: Removing the old repo with `rm -rf` and re-copying ensures clean source. The `target/` directory inside the container is *not* preserved across `rm -rf` + `docker cp` since it lives under `/sonic-dash-ha/target/`. For faster incremental builds, consider syncing only changed files instead.
- **Long output**: Build and test output can be very large. Use `| tail -n 50` if you only need to see the summary, but show full output on failure.
- **Timeouts**: Use generous timeouts (300s+) for build commands. Full `ci-all` can take much longer.
- **Release builds**: If the user asks to test release mode, add `--release` to build and test commands.
