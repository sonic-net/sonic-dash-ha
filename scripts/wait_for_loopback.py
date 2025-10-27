#!/usr/bin/env python3

"""
    wait_for_loopback.py

    Script to wait for LOOPBACK_INTERFACE to be programmed in APPL_DB.

    This script polls APPL_DB to check if the LOOPBACK_INTERFACE interface
    has been programmed. It will continue polling until the interface is found
    or a maximum number of retries is reached.
"""

import sys
import time
import syslog
from swsscommon import swsscommon

# Configuration
LOOPBACK_INTERFACE = "Loopback0"
MAX_RETRIES = 300  # Maximum number of retries
POLL_INTERVAL = 1  # Poll interval in seconds
REDIS_TIMEOUT_MS = 0


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
    Check if LOOPBACK_INTERFACE interface is programmed in APPL_DB.

    Returns:
        bool: True if LOOPBACK_INTERFACE interface exists in APPL_DB, False otherwise.
    """
    try:
        # Connect to APPL_DB
        appl_db = swsscommon.DBConnector("APPL_DB", REDIS_TIMEOUT_MS, False)
        intf_table = swsscommon.Table(appl_db, "INTF_TABLE")
        keys = intf_table.getKeys()

        # Check for LOOPBACK_INTERFACE
        for key in keys:
            if key.startswith(LOOPBACK_INTERFACE):
                log_debug(f"Found key: {key}")
                if key == LOOPBACK_INTERFACE or "|" in key:
                    return True
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

        if retry_count % 10 == 0:
            log_info(f"Waiting for {LOOPBACK_INTERFACE}... (attempt {retry_count}/{MAX_RETRIES})")

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
