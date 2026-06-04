"""Smoke tests — no deployment required, completes in ~30s."""

from scripts.tests.conftest import (
    ensure_confluent_cli_installed,
    ensure_confluent_login,
    load_test_credentials,
)


def test_confluent_cli_installed() -> None:
    """Fail fast if the confluent CLI is not on PATH."""
    ensure_confluent_cli_installed()


def test_credentials_load() -> None:
    """credentials.env exists and all required TF_VAR_* keys are populated."""
    credentials = load_test_credentials()
    required = ["cloud", "region", "confluent_cloud_api_key", "confluent_cloud_api_secret"]
    missing = [k for k in required if not credentials.get(k)]
    assert not missing, f"Missing required credential keys: {missing}"


def test_confluent_login() -> None:
    """Confluent CLI is authenticated (or can be authenticated)."""
    credentials = load_test_credentials()
    ensure_confluent_login(credentials)
