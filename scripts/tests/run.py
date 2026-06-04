"""Entry points for running the test suite."""

import sys

import pytest


def full() -> None:
    """Run the full E2E suite (deploys, runs Labs 1-3, tears down)."""
    sys.exit(
        pytest.main(
            [
                "scripts/tests/e2e/test_sql_extraction.py",
                "scripts/tests/e2e/test_lab1.py",
                "scripts/tests/e2e/test_lab2.py",
                "scripts/tests/e2e/test_lab3.py",
                "-v",
                "--timeout=3600",
            ]
        )
    )
