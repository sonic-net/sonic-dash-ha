---
name: deploy-dash-ha
description: 'Build dash-ha debian packages and deploy to SONiC switches. Use when: building deb, deploying to switches, installing dash-ha, config reload, dpkg-buildpackage, deploy dash-hadpu containers.'
argument-hint: 'Optional: switch IPs, credentials, or container names to customize deployment'
---

# Deploy dash-ha to SONiC Switches

## When to Use
- After code changes, to build and deploy updated dash-ha binaries
- When installing dash-ha packages on switches

## Prerequisites
- The `sonic-dash-ha` Docker build container must be running locally (check with `docker ps`)
- Target switches must be reachable via SSH
- SSH access to the switches is configured. **Strongly preferred:** key-based
  authentication (an SSH public key installed on each switch, e.g. via
  `ssh-copy-id admin@<switch_ip>`). If only password auth is available, `ssh`/`scp`
  will prompt for the password interactively — the user types it directly into the
  terminal.

## Default Configuration

| Parameter | Default Value |
|-----------|--------------|
| Build container | `sonic-dash-ha` |
| Host repo path | `<path-to-sonic-dash-ha>` |
| Container repo path | `/sonic-dash-ha` |
| Switch IPs | `<switch_ip>` (one or more) |
| Switch username | `admin` |
| Target containers | `dash-hadpu0`, `dash-hadpu1`, `dash-hadpu2`, `dash-hadpu3` |
| Packages | `dash-ha_1.0.0_amd64.deb`, `dash-ha-dbgsym_1.0.0_amd64.deb` |

The values above are placeholders. Ask the user (or read from their environment)
for the host repo path, switch IPs, username, and container names, and use those instead.

## Procedure

### Step 0: Confirm SSH access

Use key-based SSH authentication wherever possible (install your public key on each
switch with `ssh-copy-id admin@<switch_ip>`). **Never** pass passwords on the command
line (no `sshpass`, no `-p` flags) and never store or hardcode credentials. If a switch
only supports password auth, run the `ssh`/`scp` commands below as-is and let the user
type the password directly into the terminal when prompted.

### Step 1: Copy repo into build container

```bash
docker exec sonic-dash-ha bash -c "rm -rf /sonic-dash-ha" && \
docker cp <path-to-sonic-dash-ha> sonic-dash-ha:/sonic-dash-ha
```

### Step 2: Build debian packages

```bash
docker exec sonic-dash-ha bash -c "cd /sonic-dash-ha && dpkg-buildpackage -us -uc -b 2>&1" | tail -5
```

Verify the output ends with `dpkg-buildpackage: info: binary-only upload`. If the build fails, show the full error output.

### Step 3: Extract debs and SCP to switches

```bash
# Extract from container
docker cp sonic-dash-ha:/dash-ha_1.0.0_amd64.deb /tmp/dash-ha_1.0.0_amd64.deb
docker cp sonic-dash-ha:/dash-ha-dbgsym_1.0.0_amd64.deb /tmp/dash-ha-dbgsym_1.0.0_amd64.deb

# SCP to all switches
scp -o StrictHostKeyChecking=no \
  /tmp/dash-ha_1.0.0_amd64.deb /tmp/dash-ha-dbgsym_1.0.0_amd64.deb \
  admin@<switch_ip>:/tmp/
```

### Step 4: Install in dash-hadpu containers on each switch

For each switch, run:

```bash
ssh -o StrictHostKeyChecking=no admin@<switch_ip> \
  'for i in 0 1 2 3; do
    echo "=== dash-hadpu$i ===";
    sudo docker cp /tmp/dash-ha_1.0.0_amd64.deb dash-hadpu$i:/dash-ha_1.0.0_amd64.deb;
    sudo docker cp /tmp/dash-ha-dbgsym_1.0.0_amd64.deb dash-hadpu$i:/dash-ha-dbgsym_1.0.0_amd64.deb;
    sudo docker exec dash-hadpu$i dpkg -i /dash-ha_1.0.0_amd64.deb /dash-ha-dbgsym_1.0.0_amd64.deb;
  done'
```

**Important**: Copy files to `/` inside the containers, not `/tmp/` — the containers use a tmpfs for `/tmp/` that doesn't persist across `docker cp` and `docker exec`.

Verify each container shows `Setting up dash-ha (1.0.0) ...` and `Setting up dash-ha-dbgsym (1.0.0) ...`.

### Step 5: Config reload on each switch

```bash
ssh -o StrictHostKeyChecking=no admin@<switch_ip> \
  'sudo config reload -y 2>&1'
```

Verify output shows `Released lock on /etc/sonic/reload.lock`.

## Gotchas

- **Container `/tmp/` is tmpfs**: Files copied via `docker cp` to `/tmp/` inside dash-hadpu containers are not visible to `docker exec`. Copy to `/` instead (e.g., `/dash-ha_1.0.0_amd64.deb`).
- **Build cache**: The build container preserves cargo cache from previous builds under `/sonic-dash-ha/target/`, so incremental builds are fast. Removing the old repo with `rm -rf` and re-copying ensures clean source while preserving no stale build artifacts.
- **Debug symbols**: Always install `dash-ha-dbgsym` alongside `dash-ha` unless explicitly told otherwise.
- **Config reload timeout**: The `config reload` command can take a while. Use a generous timeout (300s+).
