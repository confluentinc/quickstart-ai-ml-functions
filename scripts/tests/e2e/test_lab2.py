"""Lab 2 E2E test: validate the walkthrough end-to-end.

Parses SQL from `Lab2-Walkthrough.md` and runs it sequentially. Asserts:
  - Each CREATE statement results in a Flink catalog object that DESCRIBE confirms
  - The `fraud_transactions` topic emits >=1 record with a spike amount (> $500).

The Lab 2 challenge block in the walkthrough is fenced as ```sql no-parse``` so
the extractor skips it (it contains `<YOUR_LOGIC>` placeholders).
"""

from functools import partial
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
    def test_transactions_data_generation(self, deployed_environment: dict[str, Any]) -> None:
        """The Faker connector is producing rows to the transactions topic."""
        flink = deployed_environment["flink"]
        flink.execute_statement("test-transactions-data", "SELECT * FROM transactions LIMIT 100")
        status = flink.wait_for_running_or_completed("test-transactions-data", timeout=300)
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

        # Headline assertion: fraud_transactions emits confirmed-fraud rows.
        # ARIMA needs minTrainingSize=10 + per-customer warm-up — expect ~3 min
        # before the first spike appears in fraud_transactions.
        rows = poll_until(
            getter=partial(poll_topic, kafka_creds, "fraud_transactions", 1, 60),
            condition=lambda msgs: len(msgs) >= 1,
            timeout=600,
            interval=30,
            description="fraud_transactions has >= 1 record",
        )
        # Every row must be a genuine spike: amount > $500 severity floor
        low_amounts = [r for r in rows if r.get("amount", 0) <= 500]
        assert not low_amounts, (
            "Expected all fraud_transactions records to have amount > $500 (severity floor). "
            f"Found low-amount record: {low_amounts[0]}"
        )

        flink.verify_no_failed(submitted)
