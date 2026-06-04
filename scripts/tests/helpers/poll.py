"""Polling utility for test assertions."""

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def poll_until(
    getter: Callable[[], T],
    condition: Callable[[T], bool],
    timeout: int = 300,
    interval: int = 5,
    description: str = "condition",
) -> T:
    """Poll until condition is met or timeout.

    Args:
        getter: Function that returns a value to check
        condition: Function that validates the value (returns True if valid)
        timeout: Maximum time to wait in seconds (default: 300s)
        interval: Time between polls in seconds (default: 5s)
        description: Human-readable description for error messages

    Returns:
        The value from getter when condition is met

    Raises:
        TimeoutError: If condition not met within timeout
    """
    start_time = time.time()
    attempt = 0

    while True:
        attempt += 1
        elapsed = time.time() - start_time

        try:
            value = getter()

            if condition(value):
                return value

            if elapsed >= timeout:
                raise TimeoutError(
                    f"Timeout waiting for {description} after {elapsed:.1f}s ({attempt} attempts). Last value: {value}"
                )

            time.sleep(interval)

        except Exception as e:
            if isinstance(e, TimeoutError):
                raise

            if elapsed >= timeout:
                raise TimeoutError(f"Error polling for {description} after {elapsed:.1f}s: {e}") from e

            time.sleep(interval)
