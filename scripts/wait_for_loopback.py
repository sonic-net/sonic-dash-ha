#!/usr/bin/env python3

"""
    wait_for_loopback.py

    Script to wait for LOOPBACK_INTERFACE to be configured.

    This script polls the loopback interface using ip and ip -6 commands
    to check if the interface exists and has IP addresses assigned.
    It will continue polling until the interface is found with addresses
    or a maximum number of retries is reached.
"""

import sys
import time
import syslog
import subprocess

# Configuration
LOOPBACK_INTERFACE = "Loopback0"
MAX_RETRIES = 300  # Maximum number of retries
POLL_INTERVAL = 1  # Poll interval in seconds


def log_info(msg):
    """Log info message to syslog"""
    syslog.syslog(syslog.LOG_INFO, f"wait_for_loopback: {msg}")


def log_err(msg):
    """Log error message to syslog"""
    syslog.syslog(syslog.LOG_ERR, f"wait_for_loopback: {msg}")


def log_debug(msg):
    """Log debug message to syslog"""
    syslog.syslog(syslog.LOG_DEBUG, f"wait_for_loopback: {msg}")


def check_loopback_interface():
    """
    Check if LOOPBACK_INTERFACE interface is configured with IP addresses.

    Returns:
        bool: True if LOOPBACK_INTERFACE interface exists and has IP addresses, False otherwise.
    """
    try:
        # Check IPv4 addresses
        result_ipv4 = subprocess.run(
            ["ip", "-4", "addr", "show", LOOPBACK_INTERFACE],
            capture_output=True,
            text=True,
            timeout=5
        )

        # Check IPv6 addresses
        result_ipv6 = subprocess.run(
            ["ip", "-6", "addr", "show", LOOPBACK_INTERFACE],
            capture_output=True,
            text=True,
            timeout=5
        )

        # Check if interface exists (return code 0)
        if result_ipv4.returncode != 0 and result_ipv6.returncode != 0:
            log_debug(f"{LOOPBACK_INTERFACE} interface not found")
            return False

        # Check if there are any IP addresses configured
        has_ipv4 = "inet " in result_ipv4.stdout
        has_ipv6 = "inet6 " in result_ipv6.stdout

        if has_ipv4 or has_ipv6:
            if has_ipv4:
                log_debug(f"Found IPv4 addresses on {LOOPBACK_INTERFACE}")
            if has_ipv6:
                log_debug(f"Found IPv6 addresses on {LOOPBACK_INTERFACE}")
            return True
        else:
            log_debug(f"{LOOPBACK_INTERFACE} interface exists but has no IP addresses")
            return False

    except subprocess.TimeoutExpired:
        log_err(f"Timeout checking {LOOPBACK_INTERFACE} interface")
        return False
    except Exception as e:
        log_err(f"Error checking loopback interface: {e}")
        return False


def wait_for_loopback():
    """
    Main function to wait for LOOPBACK_INTERFACE to be programmed.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    log_info(f"Wait for {LOOPBACK_INTERFACE} interface...")

    retry_count = 0

    while retry_count < MAX_RETRIES:
        if check_loopback_interface():
            log_info(f"{LOOPBACK_INTERFACE} interface is programmed")
            return 0

        retry_count += 1

        time.sleep(POLL_INTERVAL)

    log_err(f"{LOOPBACK_INTERFACE} interface was not programmed after {MAX_RETRIES} retries")
    return 1


def main():
    """Main entry point"""
    syslog.openlog("wait_for_loopback", syslog.LOG_PID)

    try:
        exit_code = wait_for_loopback()
        sys.exit(exit_code)
    except Exception as e:
        log_err(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
