"""Pytest configuration and shared fixtures for E2E tests."""

import shutil
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from dotenv import dotenv_values

from scripts.common.login_checks import ensure_confluent_login as _ensure_confluent_login_shared
from scripts.common.terraform import extract_kafka_credentials
from scripts.tests.helpers.flink_helper import FlinkHelper

PROJECT_ROOT = Path(__file__).parent.parent.parent
TESTS_DIR = Path(__file__).parent


def load_test_credentials() -> dict[str, str]:
    """Load credentials from credentials.env at the project root.

    Normalizes TF_VAR_* keys to plain names expected by the test suite.

    Optional keys for automated CLI login:
        CONFLUENT_USERNAME / CONFLUENT_PASSWORD

    Returns:
        Normalized credentials dict with keys:
            cloud, region, confluent_cloud_api_key, confluent_cloud_api_secret,
            and optionally confluent_username, confluent_password

    Raises:
        FileNotFoundError: If credentials.env is not found
        ValueError: If required fields are missing
    """
    creds_file = PROJECT_ROOT / "credentials.env"
    if not creds_file.exists():
        raise FileNotFoundError(
            f"credentials.env not found at {creds_file}\n"
            "Create it from credentials.env.example and fill in your API keys."
        )

    raw = dotenv_values(creds_file)

    credentials: dict[str, str] = {
        "cloud": raw.get("TF_VAR_cloud_provider", ""),
        "region": raw.get("TF_VAR_cloud_region", ""),
        "confluent_cloud_api_key": raw.get("TF_VAR_confluent_cloud_api_key", ""),
        "confluent_cloud_api_secret": raw.get("TF_VAR_confluent_cloud_api_secret", ""),
    }

    # Optional: CLI login credentials (not required if already logged in)
    # CONFLUENT_EMAIL preferred; CONFLUENT_USERNAME accepted as legacy alias.
    if raw.get("CONFLUENT_EMAIL"):
        credentials["confluent_email"] = raw["CONFLUENT_EMAIL"]
    elif raw.get("CONFLUENT_USERNAME"):
        credentials["confluent_username"] = raw["CONFLUENT_USERNAME"]
    if raw.get("CONFLUENT_PASSWORD"):
        credentials["confluent_password"] = raw["CONFLUENT_PASSWORD"]

    required = ["cloud", "region", "confluent_cloud_api_key", "confluent_cloud_api_secret"]
    missing = [k for k in required if not credentials.get(k)]
    if missing:
        key_map = {
            "cloud": "TF_VAR_cloud_provider",
            "region": "TF_VAR_cloud_region",
            "confluent_cloud_api_key": "TF_VAR_confluent_cloud_api_key",
            "confluent_cloud_api_secret": "TF_VAR_confluent_cloud_api_secret",
        }
        raise ValueError(f"Missing required fields in credentials.env: {', '.join(key_map[k] for k in missing)}")

    return credentials


def ensure_confluent_cli_installed() -> None:
    """Check that the confluent CLI is installed.

    Raises:
        pytest.skip: If confluent CLI not found
    """
    if not shutil.which("confluent"):
        pytest.skip(
            "confluent CLI not found. Install from: https://docs.confluent.io/confluent-cli/current/install.html"
        )


def ensure_confluent_login(credentials: dict[str, str]) -> None:
    """Ensure the Confluent CLI is authenticated, using the shared auto-login helper.

    Translates the test-suite credential dict (lower-case keys from
    load_test_credentials) into the dotenv-style keys the shared helper expects.
    """
    raw = {
        "CONFLUENT_EMAIL": credentials.get("confluent_email") or credentials.get("confluent_username", ""),
        "CONFLUENT_PASSWORD": credentials.get("confluent_password", ""),
    }
    _ensure_confluent_login_shared(raw)


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Get project root directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def tests_dir() -> Path:
    """Get tests directory."""
    return TESTS_DIR


@pytest.fixture(scope="session")
def deployed_environment() -> Generator[dict[str, Any], None, None]:
    """Deploy infra once for all labs, yield test state, then tear down."""
    ensure_confluent_cli_installed()
    credentials = load_test_credentials()
    ensure_confluent_login(credentials)

    flink: FlinkHelper | None = None
    try:
        result = subprocess.run(
            ["uv", "run", "deploy", "--testing"],
            cwd=PROJECT_ROOT,
            timeout=1800,
        )
        if result.returncode != 0:
            pytest.fail("Deployment failed")

        kafka_creds = extract_kafka_credentials(credentials["cloud"], PROJECT_ROOT)
        kafka_creds["cloud"] = credentials["cloud"]
        kafka_creds["region"] = credentials["region"]

        flink = FlinkHelper(kafka_creds)

        yield {"flink": flink, "kafka_creds": kafka_creds, "cloud": credentials["cloud"]}

    finally:
        if flink is not None:
            flink.cleanup_all()

        result = subprocess.run(
            ["uv", "run", "destroy", "--testing"],
            cwd=PROJECT_ROOT,
            timeout=1200,
        )
        if result.returncode != 0:
            pytest.fail("Destroy failed")
