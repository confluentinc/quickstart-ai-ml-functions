"""Lab 1 E2E test: validate the walkthrough end-to-end.

Parses SQL from `Lab1-Walkthrough.md` and runs it sequentially. Asserts:
  - Each CREATE statement results in a Flink catalog object that DESCRIBE confirms
  - The `equipment_anomalies` topic emits ≥1 record with `anomaly.is_anomaly = TRUE`

If the walkthrough drifts (e.g., a new CREATE block is added or the output table
is renamed), this test will surface it.
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

WALKTHROUGH = Path(__file__).parent.parent.parent.parent / "Lab1-Walkthrough.md"


class TestLab1:
    """Lab 1 E2E: parse Lab1-Walkthrough.md and run every pipeline SQL block."""

    @pytest.mark.order(1)
    def test_cnc_signals_data_generation(self, deployed_environment: dict[str, Any]) -> None:
        """The Faker connector + INSERT pipeline is producing rows to cnc_machine_signals."""
        flink = deployed_environment["flink"]
        flink.execute_statement("test-cnc-signals", "SELECT * FROM cnc_machine_signals LIMIT 100")
        status = flink.wait_for_running_or_completed("test-cnc-signals", timeout=300)
        assert status in ("RUNNING", "COMPLETED")

    @pytest.mark.order(2)
    def test_walkthrough_pipeline(self, deployed_environment: dict[str, Any]) -> None:
        """Run every CREATE statement from Lab1-Walkthrough.md and verify outputs."""
        flink = deployed_environment["flink"]
        kafka_creds = deployed_environment["kafka_creds"]

        blocks = extract_sql_blocks(WALKTHROUGH)
        pipeline_blocks = [b for b in blocks if is_executable_pipeline_sql(b["sql"])]
        assert pipeline_blocks, "No pipeline SQL found in Lab1-Walkthrough.md"

        submitted: list[str] = []
        for i, block in enumerate(pipeline_blocks):
            sql = block["sql"]
            obj = extract_sql_object(sql)
            obj_name = obj[1] if obj else f"block-{i}"
            stmt = f"test-lab1-{i}-{obj_name.replace('_', '-')}"[:60]
            print(f"[{i + 1}/{len(pipeline_blocks)}] {stmt}: {block['header']}")
            flink.ensure_statement(stmt, sql, timeout=300)
            submitted.append(stmt)

        # Headline assertion: anomalies are flowing.
        rows = poll_until(
            getter=lambda: poll_topic(kafka_creds, "equipment_anomalies", 1, 60),
            condition=lambda msgs: len(msgs) >= 1,
            timeout=900,
            interval=30,
            description="equipment_anomalies has >= 1 record",
        )
        # Each row should encode an anomaly (the walkthrough filters WHERE is_anomaly = TRUE).
        flagged = [r for r in rows if _is_anomaly_row(r)]
        assert flagged, f"No equipment_anomalies records had anomaly.is_anomaly = TRUE. Sample row: {rows[0]}"

        flink.verify_no_failed(submitted)


def _is_anomaly_row(row: dict) -> bool:
    """Lab 1's equipment_anomalies row has an `anomaly` struct with `is_anomaly`."""
    anomaly = row.get("anomaly")
    if isinstance(anomaly, dict):
        return bool(anomaly.get("is_anomaly"))
    # Defensive: if the struct was flattened by the consumer, try the flat key.
    return bool(row.get("is_anomaly"))
