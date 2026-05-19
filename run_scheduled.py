#!/usr/bin/env python3
"""
Scheduled ETL runner for Windows.
Connects to the Windows built-in VPN, runs main.py, then disconnects.
Designed to be invoked by Windows Task Scheduler via run_scheduled.bat.
"""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR / ".env")

sys.path.insert(0, str(SCRIPT_DIR))
from src.utils.logger import setup_logger

VPN_CONNECTION_NAME = os.getenv("VPN_CONNECTION_NAME", "").strip()


def _rasdial(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["rasdial", *args],
        capture_output=True,
        text=True,
    )


def is_vpn_connected(name: str) -> bool:
    """Return True if the named connection is currently active."""
    result = _rasdial()  # no args → lists active connections
    return name.lower() in result.stdout.lower()


def vpn_connect(name: str, logger) -> None:
    logger.info("Connecting to VPN: %s", name)
    result = _rasdial(name)
    if result.returncode != 0:
        output = (result.stdout + result.stderr).strip()
        raise RuntimeError(f"VPN connect failed (exit {result.returncode}): {output}")
    logger.info("VPN connected successfully.")


def vpn_disconnect(name: str, logger) -> None:
    logger.info("Disconnecting from VPN: %s", name)
    result = _rasdial(name, "/disconnect")
    if result.returncode != 0:
        output = (result.stdout + result.stderr).strip()
        logger.warning(
            "VPN disconnect returned non-zero (exit %s): %s", result.returncode, output
        )


def _run_etl(logger) -> int:
    logger.info("Starting ETL pipeline.")
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "main.py")] + sys.argv[1:],
        cwd=str(SCRIPT_DIR),
    )
    if result.returncode == 0:
        logger.info("ETL pipeline finished successfully.")
    else:
        logger.error("ETL pipeline exited with code %s.", result.returncode)
    return result.returncode


def main() -> int:
    logger = setup_logger("scheduled_runner")

    if not VPN_CONNECTION_NAME:
        logger.error("VPN_CONNECTION_NAME is not set in .env — cannot continue.")
        return 1

    if is_vpn_connected(VPN_CONNECTION_NAME):
        logger.info("VPN '%s' already active, skipping connect.", VPN_CONNECTION_NAME)
        return _run_etl(logger)

    try:
        vpn_connect(VPN_CONNECTION_NAME, logger)
    except RuntimeError as e:
        logger.error(str(e))
        return 1
    else:
        try:
            return _run_etl(logger)
        finally:
            vpn_disconnect(VPN_CONNECTION_NAME, logger)


if __name__ == "__main__":
    sys.exit(main())
