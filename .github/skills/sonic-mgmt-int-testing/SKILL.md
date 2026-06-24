---
name: sonic-mgmt-int-testing
description: 'Run sonic-mgmt-int pytest tests on physical SmartSwitch testbeds and debug failures. Use when: running HA tests, planned shutdown tests, running sonic-mgmt-int tests, checking test logs, debugging test failures on physical switches, analyzing syslog on DUT, run_tests.sh.'
argument-hint: 'Optional: test file path, testbed name, DUT hostnames, or DPU hostnames'
---

# sonic-mgmt-int Physical Testbed Testing

Run pytest-based tests from the `sonic-mgmt-int` container against physical SmartSwitch testbeds, then debug failures using test logs and switch syslog.

## When to Use
- Running HA or SmartSwitch tests on physical testbeds
- Debugging test failures by checking logs and switch syslog
- Analyzing XML test results from sonic-mgmt-int

## Default Configuration

| Parameter | Default Value |
|-----------|--------------|
| Container name | `sonic-mgmt-int` |
| Tests directory | `/data/sonic-mgmt-int/tests` |
| Logs directory | `/data/sonic-mgmt-int/tests/logs` |
| Inventory files | `../ansible/<inventory>,../ansible/veos` |
| Testbed name | `<testbed_name>` |

The inventory name and testbed name are placeholders. Ask the user for their
testbed name and inventory file(s), and use those instead.

## Procedure

### Step 0: Ask for required parameters

Before starting, use the ask-questions tool to collect any missing information:
- **Test file** — the pytest file to run (e.g., `ha/test_ha_planned_shutdown.py`)
- **DUT hostnames** — comma-separated DUT names (e.g., `<dut-hostname-1>,<dut-hostname-2>`)
- **DPU hostnames** — comma-separated DPU names (e.g., `<dut-hostname-1>-dpu-0,<dut-hostname-2>-dpu-0`)
- **Switch authentication** — Use key-based SSH where possible (install your public
  key on the switch with `ssh-copy-id admin@<switch_ip>`). If only password auth is
  available, let `ssh` prompt and type the password directly into the terminal. **Never**
  pass passwords on the command line (no `sshpass`/`-p`) and never route passwords
  through ask-questions.

Use the defaults above for inventory, testbed name, and other flags unless the user specifies otherwise.

### Step 1: Enter the sonic-mgmt-int container

```bash
docker exec -it sonic-mgmt-int bash
```

All subsequent commands run **inside** this container.

### Step 2: Navigate to the tests directory

```bash
cd /data/sonic-mgmt-int/tests
```

### Step 3: Run the test

Use `run_tests.sh` with the collected parameters:

```bash
./run_tests.sh \
  -i ../ansible/<inventory>,../ansible/veos \
  -n <testbed_name> \
  -e "--disable_loganalyzer --skip_yang --skip_sanity" \
  -u \
  -m individual \
  -d <DUT_HOSTNAMES> \
  -H <DPU_HOSTNAMES> \
  -c "<TEST_FILE>"
```

**Important**: This command can take a long time (10+ minutes). Use a generous timeout or run in async mode so the test can complete without interruption.

### Step 4: Wait for the test to finish

Monitor the terminal output until the test completes. Look for the pytest summary showing passed/failed/error counts.

### Step 5: Check test logs and XML results

After the test finishes, examine the logs directory:

```bash
ls -lt /data/sonic-mgmt-int/tests/logs/ | head -10
```

Find the most recent log directory (named by timestamp or test name). Inside it, check:

- **`*.xml`** — JUnit XML results. Parse for `<failure>` and `<error>` elements to identify which tests failed and why.
- **`*.log`** — Detailed test execution logs. Search for `FAILED`, `ERROR`, `assert`, or exception tracebacks.

```bash
# Find the latest log directory
LATEST=$(ls -td /data/sonic-mgmt-int/tests/logs/*/ | head -1)

# Show test results summary from XML
grep -E 'testcase|failure|error' "$LATEST"/*.xml | head -30

# Show failures from the log
grep -B5 -A10 'FAILED\|ERROR\|AssertionError' "$LATEST"/*.log | tail -50
```

### Step 6: Check syslog on the switches

To debug failures, connect to the DUT switches and inspect syslog. First, look up the switch management IPs from the inventory:

```bash
# Inside sonic-mgmt-int container
grep -A5 '<DUT_HOSTNAME>' /data/sonic-mgmt-int/ansible/<inventory>
```

Then SSH to the switch (from the dev machine, not the container) and check syslog.
Use key-based auth if configured; otherwise `ssh` prompts for the password interactively:

```bash
ssh -o StrictHostKeyChecking=no admin@<SWITCH_IP> \
  "sudo grep -E 'ERR|WARN|CRIT' /var/log/syslog | tail -50"
```

For DPU-specific logs (dash-hadpu containers):

```bash
ssh -o StrictHostKeyChecking=no admin@<SWITCH_IP> \
  "sudo grep 'dash-hadpu0#hamgrd' /var/log/syslog | grep -v DEBUG | tail -30"
```

## Gotchas

- **Long test times**: Tests on physical testbeds can take 10-30+ minutes. Always use generous timeouts.
- **Inventory paths**: The `-i` flag takes comma-separated paths to Ansible inventory directories. The inventory files contain the management IPs for each switch.
- **`-u` flag**: Passes `--allow-unsorted` to pytest, required for most test runs.
- **`-m individual`**: Runs tests one at a time instead of in parallel.
- **`-H` flag**: Specifies DPU hostnames for SmartSwitch HA tests.
- **Log rotation**: Switch syslogs may rotate. Check `/var/log/syslog.1` if recent entries are missing.
- **Container syslog**: hamgrd logs inside dash-hadpu containers are forwarded to the host's `/var/log/syslog` prefixed with `dash-hadpuN#hamgrd`.
