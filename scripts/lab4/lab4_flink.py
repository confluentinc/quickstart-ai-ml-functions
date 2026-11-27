#!/usr/bin/env python3
"""
Lab 4 presenter shortcut: run walkthrough steps 2 and 3 for you.

Submits the two pipeline statements from `Lab4-Walkthrough.md` (the single
source of truth for Lab 4 SQL) and waits until each one has been accepted.
Safe to re-run: the statements are `CREATE OR ALTER MATERIALIZED TABLE`.

Usage: uv run lab4-flink
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

from scripts.common.sql_extractors import extract_sql_blocks, extract_sql_object, is_executable_pipeline_sql
from scripts.common.terraform import get_project_root, run_terraform_output

WALKTHROUGH = "Lab4-Walkthrough.md"
TIMEOUT_SECONDS = 180


def pipeline_statements(root: Path) -> list[tuple[str, str]]:
    """(table name, SQL) for every pipeline block in the walkthrough, in order."""
    blocks = [b for b in extract_sql_blocks(root / WALKTHROUGH) if is_executable_pipeline_sql(b["sql"])]
    statements = []
    for block in blocks:
        obj = extract_sql_object(block["sql"])
        if obj is None:
            raise RuntimeError(f"Could not find a table name in the SQL under '{block['header']}'")
        statements.append((obj[1], block["sql"]))
    return statements


def _cli(*args: str, flink: dict[str, str]) -> subprocess.CompletedProcess[str]:
    cmd = ["confluent", "flink", "statement", *args, "--environment", flink["env"]]
    cmd += ["--cloud", flink["cloud"], "--region", flink["region"]]
    return subprocess.run(cmd, capture_output=True, text=True)


def _phase(name: str, flink: dict[str, str]) -> tuple[str, str]:
    result = _cli("describe", name, "-o", "json", flink=flink)
    if result.returncode != 0:
        return "NOT_FOUND", result.stderr.strip()
    data = json.loads(result.stdout)
    return str(data.get("status", "")).upper(), str(data.get("status_detail") or "")


def submit(name: str, sql: str, flink: dict[str, str]) -> None:
    """Create the statement (replacing an earlier run's) and wait until Flink accepts it."""
    if _phase(name, flink)[0] != "NOT_FOUND":
        _cli("delete", name, "--force", flink=flink)

    created = subprocess.run(
        [
            "confluent", "flink", "statement", "create", name,
            "--sql", sql,
            "--compute-pool", flink["pool"],
            "--environment", flink["env"],
            "--database", flink["database"],
        ],
        capture_output=True,
        text=True,
    )  # fmt: skip
    if created.returncode != 0:
        raise RuntimeError(created.stderr.strip() or created.stdout.strip())

    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        phase, detail = _phase(name, flink)
        if phase in ("COMPLETED", "RUNNING"):
            return
        if phase in ("FAILED", "STOPPED", "DEGRADED"):
            hint = ""
            if "AI_FORECAST" in detail or "AI_DETECT_ANOMALIES" in detail or "not enabled" in detail.lower():
                hint = "\n  Is the AI_FORECAST / AI_DETECT_ANOMALIES Early Access enabled for your organization?"
            raise RuntimeError(f"{phase}: {detail}{hint}")
        time.sleep(3)
    raise RuntimeError(f"Still not running after {TIMEOUT_SECONDS}s. Check Confluent Cloud → Flink → Statements.")


def main() -> None:
    root = get_project_root()
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        print("✗ Nothing deployed yet. Run: uv run deploy lab4")
        sys.exit(1)

    outputs = run_terraform_output(core_state)
    flink = {
        "env": outputs["confluent_environment_id"],
        "pool": outputs["confluent_flink_compute_pool_id"],
        "database": outputs["confluent_kafka_cluster_display_name"],
        "cloud": outputs["cloud_provider"],
        "region": outputs["cloud_region"],
    }

    statements = pipeline_statements(root)
    print(f"Running {len(statements)} statements from {WALKTHROUGH}\n")
    for i, (table, sql) in enumerate(statements, 1):
        name = f"lab4-{table.replace('_', '-')}"
        print(f"[{i}/{len(statements)}] {table} ...", end=" ", flush=True)
        try:
            submit(name, sql, flink)
        except RuntimeError as e:
            print(f"failed\n  {e}")
            sys.exit(1)
        print("✓")

    print("\n✓ Both pipelines are running. Next: uv run lab4-dashboard")


if __name__ == "__main__":
    main()
