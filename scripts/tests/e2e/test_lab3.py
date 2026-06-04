"""Lab 3 E2E test: validate the walkthrough end-to-end.

Lab 3 differs from Labs 1/2: it needs a Python datagen process running before
the Flink CTAS pipelines have data to read. This test:

  1. Starts `uv run lab3-datagen --scenario peak` as a background subprocess
     (peak scenario guarantees alerts fire regardless of wall-clock UTC time).
  2. Waits for `lab3_tower_traffic` to receive enough messages for ARIMA to warm up.
  3. Submits every CREATE statement from Lab3-Walkthrough.md sequentially.
  4. Polls `lab3_capacity_alerts` for >=1 alert with forecast_utilization_pct > 85.
  5. Cleans up the datagen subprocess.

ARIMA needs minTrainingSize=10 windows x 10s = ~100s of data per tower, so the
datagen's --backfill-minutes 60 (default) seeds enough history immediately.
"""

import os
import signal
import subprocess
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from scripts.common.sql_extractors import (
    extract_sql_blocks,
    extract_sql_object,
    is_executable_pipeline_sql,
)
from scripts.tests.helpers.kafka_helper import poll_topic
from scripts.tests.helpers.poll import poll_until

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
WALKTHROUGH = PROJECT_ROOT / "Lab3-Walkthrough.md"


@pytest.fixture(scope="class")
def lab3_datagen(deployed_environment: dict[str, Any]) -> Generator[subprocess.Popen, None, None]:
    """Start lab3-datagen --scenario peak in the background; clean up on exit."""
    print("Starting lab3-datagen --scenario peak in background...")
    proc = subprocess.Popen(
        ["uv", "run", "lab3-datagen", "--scenario", "peak"],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        # New process group so we can SIGINT the whole thing cleanly
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )
    try:
        # Give datagen a moment to start producing
        time.sleep(5)
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout else ""
            pytest.fail(f"lab3-datagen exited immediately:\n{output}")
        yield proc
    finally:
        print("Stopping lab3-datagen...")
        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            else:
                proc.send_signal(signal.SIGINT)
            proc.wait(timeout=30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            proc.kill()
            proc.wait(timeout=10)


class TestLab3:
    """Lab 3 E2E: datagen + parse Lab3-Walkthrough.md + run every pipeline SQL block."""

    @pytest.mark.order(1)
    def test_tower_traffic_data_generation(
        self,
        deployed_environment: dict[str, Any],
        lab3_datagen: subprocess.Popen,
    ) -> None:
        """Wait until lab3_tower_traffic has enough rows for ARIMA to warm up."""
        kafka_creds = deployed_environment["kafka_creds"]
        # Need >=100 rows (10 towers x ~10 windows) for forecasts to start.
        rows = poll_until(
            getter=lambda: poll_topic(kafka_creds, "lab3_tower_traffic", 100, 60),
            condition=lambda msgs: len(msgs) >= 100,
            timeout=300,
            interval=15,
            description="lab3_tower_traffic has >= 100 rows",
        )
        print(f"lab3_tower_traffic primed with {len(rows)} rows")

    @pytest.mark.order(2)
    def test_walkthrough_pipeline(
        self,
        deployed_environment: dict[str, Any],
        lab3_datagen: subprocess.Popen,
    ) -> None:
        """Run every CREATE statement from Lab3-Walkthrough.md and verify alerts fire."""
        flink = deployed_environment["flink"]
        kafka_creds = deployed_environment["kafka_creds"]

        blocks = extract_sql_blocks(WALKTHROUGH)
        pipeline_blocks = [b for b in blocks if is_executable_pipeline_sql(b["sql"])]
        assert pipeline_blocks, "No pipeline SQL found in Lab3-Walkthrough.md"

        submitted: list[str] = []
        for i, block in enumerate(pipeline_blocks):
            sql = block["sql"]
            obj = extract_sql_object(sql)
            obj_name = obj[1] if obj else f"block-{i}"
            stmt = f"test-lab3-{i}-{obj_name.replace('_', '-')}"[:60]
            print(f"[{i + 1}/{len(pipeline_blocks)}] {stmt}: {block['header']}")
            flink.ensure_statement(stmt, sql, timeout=300)
            submitted.append(stmt)

        # Headline assertion: alerts should fire (peak scenario keeps utilization ~87%).
        rows = poll_until(
            getter=lambda: poll_topic(kafka_creds, "lab3_capacity_alerts", 1, 60),
            condition=lambda msgs: len(msgs) >= 1,
            timeout=600,
            interval=30,
            description="lab3_capacity_alerts has >= 1 alert",
        )
        breached = [r for r in rows if (r.get("forecast_utilization_pct") or 0) > 85]
        assert breached, f"No alerts had forecast_utilization_pct > 85. Sample row: {rows[0]}"

        flink.verify_no_failed(submitted)
