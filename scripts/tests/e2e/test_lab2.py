"""Lab 2 E2E test: validate the walkthrough end-to-end.

Parses SQL from `Lab2-Walkthrough.md` and runs it sequentially. Asserts:
  - Each CREATE statement results in a Flink catalog object that DESCRIBE confirms
  - The `payments_flagged` topic emits >=1 record where at least one of the two
    anomaly signals fired (amount or cash-advance frequency).

The Lab 2 challenge block in the walkthrough is fenced as ```sql no-parse``` so
the extractor skips it (it contains `<YOUR_LOGIC>` placeholders).
"""

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

WALKTHROUGH = Path(__file__).parent.parent.parent.parent / "Lab2-Walkthrough.md"


class TestLab2:
    """Lab 2 E2E: parse Lab2-Walkthrough.md and run every pipeline SQL block."""

    @pytest.mark.order(1)
    def test_payments_data_generation(self, deployed_environment: dict[str, Any]) -> None:
        """The Faker connector is producing rows to the payments topic."""
        flink = deployed_environment["flink"]
        flink.execute_statement("test-payments-data", "SELECT * FROM payments LIMIT 100")
        status = flink.wait_for_running_or_completed("test-payments-data", timeout=300)
        assert status in ("RUNNING", "COMPLETED")

    @pytest.mark.order(2)
    def test_walkthrough_pipeline(self, deployed_environment: dict[str, Any]) -> None:
        """Run every CREATE statement from Lab2-Walkthrough.md and verify outputs."""
        flink = deployed_environment["flink"]
        kafka_creds = deployed_environment["kafka_creds"]

        blocks = extract_sql_blocks(WALKTHROUGH)
        pipeline_blocks = [b for b in blocks if is_executable_pipeline_sql(b["sql"])]
        assert pipeline_blocks, "No pipeline SQL found in Lab2-Walkthrough.md"

        submitted: list[str] = []
        for i, block in enumerate(pipeline_blocks):
            sql = block["sql"]
            obj = extract_sql_object(sql)
            obj_name = obj[1] if obj else f"block-{i}"
            stmt = f"test-lab2-{i}-{obj_name.replace('_', '-')}"[:60]
            print(f"[{i + 1}/{len(pipeline_blocks)}] {stmt}: {block['header']}")
            flink.ensure_statement(stmt, sql, timeout=300)
            submitted.append(stmt)

        # Headline assertion: anomalies are flowing.
        # ARIMA needs minTrainingSize=10 + per-customer warm-up - expect 2-3 min.
        rows = poll_until(
            getter=lambda: poll_topic(kafka_creds, "payments_flagged", 1, 60),
            condition=lambda msgs: len(msgs) >= 1,
            timeout=600,
            interval=30,
            description="payments_flagged has >= 1 record",
        )
        flagged = [r for r in rows if r.get("is_amount_anomaly") is True or r.get("is_cash_advance_anomaly") is True]
        assert flagged, (
            "No payments_flagged records had is_amount_anomaly=True or "
            f"is_cash_advance_anomaly=True. Sample row: {rows[0]}"
        )

        flink.verify_no_failed(submitted)
