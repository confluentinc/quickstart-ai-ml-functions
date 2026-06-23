"""Smoke test for scripts/common/sql_extractors.

Guards against walkthrough edits that accidentally break the SQL parser
(e.g., introducing typos in code-fence syntax). Runs in <1 second; no
infrastructure required.
"""

from pathlib import Path

import pytest

from scripts.common.sql_extractors import (
    extract_sql_blocks,
    extract_sql_object,
    is_executable_pipeline_sql,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


@pytest.mark.parametrize(
    "walkthrough,expected_tables",
    [
        ("Lab1-Walkthrough.md", ["machine_health_features", "equipment_anomalies"]),
        ("Lab2-Walkthrough.md", ["transaction_features", "flagged_windows", "fraud_transactions"]),
        ("Lab3-Walkthrough.md", ["lab3_tower_agg", "lab3_forecasts", "lab3_capacity_alerts"]),
    ],
)
def test_walkthrough_yields_expected_tables(walkthrough: str, expected_tables: list[str]) -> None:
    """Each walkthrough has exactly the expected CREATE statements for its pipeline.

    Exact equality (not subset) so that an accidental revert that re-adds or
    drops a pipeline stage fails CI.
    """
    blocks = extract_sql_blocks(PROJECT_ROOT / walkthrough)
    pipeline = [b for b in blocks if is_executable_pipeline_sql(b["sql"])]

    created = []
    for block in pipeline:
        obj = extract_sql_object(block["sql"])
        if obj:
            created.append(obj[1])

    assert created == expected_tables, (
        f"{walkthrough}: expected pipeline to create exactly {expected_tables}, got {created}"
    )


def test_lab2_challenge_block_is_skipped() -> None:
    """The Lab 2 challenge block has a `<YOUR_LOGIC>` placeholder and is
    fenced ```sql no-parse``` so the extractor skips it. If extract_sql_blocks
    starts returning it, the no-parse fence is broken."""
    blocks = extract_sql_blocks(PROJECT_ROOT / "Lab2-Walkthrough.md")
    sql_text = "\n".join(b["sql"] for b in blocks)
    assert "<YOUR_LOGIC>" not in sql_text, (
        "Lab 2 challenge skeleton leaked into extracted SQL — the `sql no-parse` "
        "fence may have been removed from the walkthrough."
    )


def test_uses_materialized_table_consistently() -> None:
    """Every CREATE statement in every walkthrough uses CREATE OR ALTER MATERIALIZED TABLE.

    Plain CREATE TABLE in a walkthrough is a regression — the labs were standardized
    on materialized tables for consistency.
    """
    for walkthrough in ("Lab1-Walkthrough.md", "Lab2-Walkthrough.md", "Lab3-Walkthrough.md"):
        blocks = extract_sql_blocks(PROJECT_ROOT / walkthrough)
        for block in blocks:
            head = block["sql"].lstrip().upper()
            if head.startswith("CREATE TABLE") and not head.startswith("CREATE TABLE IF NOT EXISTS"):
                pytest.fail(
                    f"{walkthrough}: found plain `CREATE TABLE` — should be "
                    f"`CREATE OR ALTER MATERIALIZED TABLE`. SQL starts with:\n{block['sql'][:200]}"
                )
