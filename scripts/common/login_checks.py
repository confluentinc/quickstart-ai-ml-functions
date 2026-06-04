"""Confluent CLI login utilities with non-interactive auto-login from credentials.env.

Three layers:
  - `check_confluent_login()` — am I logged in right now?
  - `attempt_confluent_auto_login(creds)` — pipe CONFLUENT_EMAIL/CONFLUENT_PASSWORD
    into `confluent login --save`; returns True on success.
  - `ensure_confluent_login(creds=None)` — combines the two: check → auto-login →
    exit(1) with actionable instructions if both fail. Use this from deploy.py
    and the test conftest so they share one path.

`CONFLUENT_USERNAME` is accepted as a legacy alias for `CONFLUENT_EMAIL` so
existing local credentials.env files keep working.
"""

import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values


def check_confluent_login() -> bool:
    """Return True if the Confluent CLI is currently authenticated."""
    try:
        result = subprocess.run(
            ["confluent", "environment", "list"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def attempt_confluent_auto_login(creds: dict) -> bool:
    """Try to log into Confluent Cloud using CONFLUENT_EMAIL + CONFLUENT_PASSWORD.

    Args:
        creds: Dict (typically from dotenv_values) — read CONFLUENT_EMAIL (or
               legacy CONFLUENT_USERNAME) and CONFLUENT_PASSWORD.

    Returns:
        True if login succeeded, False if credentials are missing or login failed.
    """
    email = creds.get("CONFLUENT_EMAIL") or creds.get("CONFLUENT_USERNAME")
    password = creds.get("CONFLUENT_PASSWORD")
    if not email or not password:
        return False

    result = subprocess.run(
        ["confluent", "login", "--save"],
        input=f"{email}\n{password}\n",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  Auto-login failed (exit {result.returncode}):")
        if result.stderr.strip():
            print(f"  {result.stderr.strip()}")
        if result.stdout.strip():
            print(f"  {result.stdout.strip()}")
        return False
    return check_confluent_login()


def ensure_confluent_login(creds: dict | None = None) -> None:
    """Check login → attempt auto-login from creds → exit(1) with clear instructions."""
    if check_confluent_login():
        return
    if creds is None:
        creds_file = Path(__file__).parent.parent.parent / "credentials.env"
        creds = dotenv_values(creds_file) if creds_file.exists() else {}
    if attempt_confluent_auto_login(creds):
        return
    print("\nError: Not logged into Confluent Cloud.")
    print("Please run: confluent login")
    print("  (or set CONFLUENT_EMAIL + CONFLUENT_PASSWORD in credentials.env for auto-login)")
    print("  (SSO accounts: run `confluent login --sso`)")
    sys.exit(1)
