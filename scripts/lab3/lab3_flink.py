#!/usr/bin/env python3
"""
Lab 3: Deploy Flink SQL pipelines after datagen has started.

Submits the CREATE statements from `Lab3-Walkthrough.md` (the single source of
truth for Lab 3 SQL) to Confluent Cloud Flink after data has been produced to
`lab3_tower_traffic`.

Usage: uv run lab3-flink
       uv run lab3-flink --wait-for-data

The walkthrough is parsed via `scripts.common.sql_extractors.extract_sql_blocks`;
plain SELECTs are skipped, only CREATE / INSERT statements are submitted.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

from scripts.common.sql_extractors import (
    extract_sql_blocks,
    extract_sql_object,
    is_executable_pipeline_sql,
)
from scripts.common.terraform import extract_kafka_credentials, get_project_root, run_terraform_output

INPUT_TOPIC = "lab3_tower_traffic"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit Lab 3 Flink pipeline SQL from Lab3-Walkthrough.md.")
    parser.add_argument(
        "--wait-for-data",
        action="store_true",
        help=f"Poll {INPUT_TOPIC} before submitting pipeline SQL.",
    )
    parser.add_argument(
        "--min-records",
        type=int,
        default=100,
        metavar="N",
        help=f"Minimum {INPUT_TOPIC} records required with --wait-for-data (default: 100).",
    )
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=300,
        metavar="SECONDS",
        help="Maximum seconds to wait for topic readiness (default: 300).",
    )
    return parser.parse_args()


def wait_for_topic_data(root: Path, min_records: int, timeout_seconds: int) -> None:
    """Wait until the Lab 3 source topic has enough records to seed the pipelines."""
    print(f"Waiting for {INPUT_TOPIC} to have at least {min_records} record(s)...")
    creds = extract_kafka_credentials("terraform", root)
    consumer = Consumer(
        {
            "bootstrap.servers": creds["bootstrap_servers"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": creds["kafka_api_key"],
            "sasl.password": creds["kafka_api_secret"],
            "group.id": f"lab3-flink-readiness-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    seen = 0
    start = time.time()
    try:
        consumer.subscribe([INPUT_TOPIC])
        while seen < min_records:
            elapsed = time.time() - start
            if elapsed >= timeout_seconds:
                print(
                    f"ERROR: Timed out after {elapsed:.0f}s waiting for {INPUT_TOPIC}. "
                    "Start `uv run lab3-datagen --scenario peak` in another terminal and retry."
                )
                sys.exit(1)

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            if msg.value() is None:
                continue

            seen += 1
            if seen == min_records or seen % 25 == 0:
                print(f"  observed {seen}/{min_records} records")
    finally:
        consumer.close()

    print(f"OK: {INPUT_TOPIC} is ready ({seen} records observed)\n")


def main() -> None:
    args = parse_args()
    print("=== Lab 3: Deploy Flink Pipelines ===\n")

    root = get_project_root()
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        print("ERROR: Core not deployed. Run 'uv run deploy' first.")
        sys.exit(1)

    outputs = run_terraform_output(core_state)
    cluster_name = outputs.get("confluent_kafka_cluster_display_name", "")
    cluster_id = outputs.get("confluent_kafka_cluster_id", "")
    compute_pool = outputs.get("confluent_flink_compute_pool_id", "")
    env_id = outputs.get("confluent_environment_id", "")

    if not all([cluster_name, cluster_id, compute_pool, env_id]):
        print("ERROR: Missing required outputs from Terraform state")
        sys.exit(1)

    walkthrough = root / "Lab3-Walkthrough.md"
    if not walkthrough.exists():
        print(f"ERROR: Walkthrough not found: {walkthrough}")
        sys.exit(1)

    blocks = extract_sql_blocks(walkthrough)
    pipeline_blocks = [b for b in blocks if is_executable_pipeline_sql(b["sql"])]
    if not pipeline_blocks:
        print(f"ERROR: No pipeline SQL found in {walkthrough.name}")
        sys.exit(1)

    print(f"Parsed {len(pipeline_blocks)} pipeline statement(s) from {walkthrough.name}:")
    for b in pipeline_blocks:
        obj = extract_sql_object(b["sql"])
        print(f"  - {obj[1] if obj else '?'}  ({b['header']})")
    print()

    if args.wait_for_data:
        wait_for_topic_data(root, args.min_records, args.wait_timeout)
    else:
        print("WARNING: Submitting without a readiness check.")
        print("   Make sure `uv run lab3-datagen` is already producing data,")
        print("   or rerun with `uv run lab3-flink --wait-for-data`.\n")

    for i, block in enumerate(pipeline_blocks, 1):
        obj = extract_sql_object(block["sql"])
        obj_name = obj[1] if obj else f"stmt-{i}"
        stmt_name = f"lab3-{obj_name.replace('_', '-')}-pipeline"
        print(f"[{i}/{len(pipeline_blocks)}] Submitting: {stmt_name}...")
        cmd = [
            "confluent",
            "flink",
            "statement",
            "create",
            stmt_name,
            "--sql",
            block["sql"],
            "--compute-pool",
            compute_pool,
            "--environment",
            env_id,
            "--database",
            cluster_name,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            err = result.stderr.strip()
            if "already exists" in err.lower():
                print("  ERROR: Statement or destination table already exists.")
                print("     A previous run likely left broken state. Clean up and retry:")
                print(f"       confluent flink statement delete {stmt_name} --environment {env_id}")
                print(f"       confluent kafka topic delete {obj_name} --cluster {cluster_id}")
                print("     Or simplest: `uv run destroy && uv run deploy` and start over.")
                sys.exit(1)
            print(f"  ERROR: Failed: {err}")
            sys.exit(1)
        print("  OK: Submitted successfully")

        if i < len(pipeline_blocks):
            time.sleep(3)  # Brief pause between statements

    print("\nOK: All Flink pipelines deployed!")
    print("\nMonitor in Confluent Cloud -> Flink -> Statements")
    print(f"or run: confluent flink statement list --environment {env_id}")


if __name__ == "__main__":
    main()
