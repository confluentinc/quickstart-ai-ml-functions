"""Lab 2 E2E test: validate the walkthrough end-to-end.

Parses SQL from `Lab2-Walkthrough.md` and runs it sequentially. Asserts:
  - Each CREATE statement results in a Flink catalog object that DESCRIBE confirms
  - Thesis: the `fraud_transactions` topic emits a SMALL_SPENDER fraud row in the
    100 < amount < 500 band — a genuine spike a global dollar threshold would miss,
    flagged only because it broke that customer's own model bound.
  - Converse: no ordinary purchase is flagged (no row inside any tier's normal band),
    so detection has not collapsed back into a global threshold. Together these two
    are the proof the detection is genuinely per-customer.

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

        # Headline assertion (thesis): fraud_transactions emits a SMALL_SPENDER spike at a
        # sub-$500 amount. SMALL_SPENDER normal tops out ~$48 and their fraud spikes are
        # ~$125-$890, so a flagged row with 100 < amount < 500 is a genuine spike a global
        # "IF amount > X" rule would have to ignore to avoid flooding on BIG_SPENDERs'
        # routine purchases. Catching it is only possible because each customer has its own
        # model — this is the proof the lab is per-customer. ARIMA needs minTrainingSize=10
        # + per-customer warm-up, so allow generous time.
        def _is_small_spike(r: dict[str, Any]) -> bool:
            return r.get("customer_segment") == "SMALL_SPENDER" and 100 < r.get("amount", 0) < 500

        rows = poll_until(
            getter=partial(poll_topic, kafka_creds, "fraud_transactions", 60, 45),
            condition=lambda msgs: any(_is_small_spike(r) for r in msgs),
            timeout=900,
            interval=20,
            description="fraud_transactions has a sub-$500 SMALL_SPENDER fraud row",
        )
        small_spike = [r for r in rows if _is_small_spike(r)]
        assert small_spike, (
            "Expected a fraud_transactions record with 100 < amount < 500 (a SMALL_SPENDER "
            "spike caught by its own model — impossible with a single global threshold). "
            f"Got {len(rows)} rows, none in that band: {rows[:3]}"
        )

        # Converse assertion: no ordinary purchase is flagged. Each tier's normal ceiling
        # sits well below its fraud spikes (SMALL ~$48 vs ~$125+, MAINSTREAM ~$480 vs
        # ~$1,250+, BIG ~$4,800 vs ~$12,500+). A flagged row inside a tier's normal band
        # would mean detection collapsed back to a global dollar threshold — the exact
        # failure this per-customer design exists to avoid.
        normal_caps = {"SMALL_SPENDER": 100, "MAINSTREAM": 1000, "BIG_SPENDER": 5000}
        normal_fps = [r for r in rows if r.get("amount", 0) < normal_caps.get(str(r.get("customer_segment", "")), 0)]
        assert not normal_fps, (
            "fraud_transactions flagged ordinary (non-fraud) purchases inside a tier's normal "
            f"band — per-customer detection is not working: {normal_fps[:5]}"
        )

        flink.verify_no_failed(submitted)
