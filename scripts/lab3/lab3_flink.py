#!/usr/bin/env python3
"""
Lab 3: Deploy Flink SQL pipelines after datagen has started.

This script submits the three CTAS pipelines (aggregation, forecasting, alerting)
to Confluent Cloud Flink after data has been produced to lab3_tower_traffic.

Usage: uv run lab3-flink

The script waits for data to exist before submitting statements,
avoiding "Column not found" errors caused by Flink's data dependency.
"""

import subprocess
import sys
import time
from pathlib import Path

from scripts.common.terraform import get_project_root, run_terraform_output


STATEMENTS = [
    {
        "name": "lab3-tower-agg-pipeline",
        "sql": """
CREATE TABLE IF NOT EXISTS `lab3_tower_agg`
DISTRIBUTED INTO 6 BUCKETS
AS
SELECT
  tower_id,
  region,
  window_start,
  window_end,
  MAX(capacity_mbps)                                 AS capacity_mbps,
  AVG(throughput_mbps)                               AS avg_throughput_mbps,
  MAX(throughput_mbps)                               AS max_throughput_mbps,
  CAST(AVG(CAST(active_users AS DOUBLE)) AS DOUBLE)  AS avg_active_users
FROM TABLE(
  TUMBLE(TABLE `lab3_tower_traffic`, DESCRIPTOR(`$rowtime`), INTERVAL '10' SECONDS)
)
GROUP BY tower_id, region, window_start, window_end;
"""
    },
    {
        "name": "lab3-forecasts-pipeline",
        "sql": """
CREATE TABLE IF NOT EXISTS `lab3_forecasts`
DISTRIBUTED INTO 6 BUCKETS
AS
SELECT
  tower_id,
  region,
  ts,
  capacity_mbps,
  avg_throughput_mbps,
  forecast[1].forecast_value AS forecast_throughput_mbps,
  forecast[1].lower_bound    AS confidence_lower_mbps,
  forecast[1].upper_bound    AS confidence_upper_mbps
FROM (
  SELECT
    tower_id,
    region,
    window_end AS ts,
    capacity_mbps,
    avg_throughput_mbps,
    ML_FORECAST(
      avg_throughput_mbps,
      window_end,
      JSON_OBJECT('horizon' VALUE 6, 'minTrainingSize' VALUE 10)
    ) OVER (
      PARTITION BY tower_id
      ORDER BY window_end
    ) AS forecast
  FROM `lab3_tower_agg`
) f
WHERE CARDINALITY(forecast) >= 1;
"""
    },
    {
        "name": "lab3-capacity-alerts-pipeline",
        "sql": """
CREATE TABLE IF NOT EXISTS `lab3_capacity_alerts`
DISTRIBUTED INTO 6 BUCKETS
AS
SELECT
  lf.tower_id,
  lf.region,
  lf.ts,
  lf.capacity_mbps,
  lf.avg_throughput_mbps,
  lf.forecast_throughput_mbps,
  ROUND(lf.forecast_throughput_mbps / lf.capacity_mbps * 100.0, 1) AS forecast_utilization_pct,
  'CAPACITY_WARNING' AS alert_type,
  CONCAT(
    'Tower ',
    lf.tower_id,
    ' forecasted at ',
    CAST(ROUND(lf.forecast_throughput_mbps / lf.capacity_mbps * 100.0, 1) AS STRING),
    '% capacity'
  ) AS alert_message
FROM `lab3_forecasts` lf
WHERE lf.forecast_throughput_mbps IS NOT NULL
  AND lf.forecast_throughput_mbps > (lf.capacity_mbps * 0.85);
"""
    },
]


def main() -> None:
    print("=== Lab 3: Deploy Flink Pipelines ===\n")

    root = get_project_root()
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        print("❌ Error: Core not deployed. Run 'uv run deploy' first.")
        sys.exit(1)

    outputs = run_terraform_output(core_state)
    cluster_name = outputs.get("confluent_kafka_cluster_display_name", "")
    compute_pool = outputs.get("confluent_flink_compute_pool_id", "")
    env_id = outputs.get("confluent_environment_id", "")

    if not all([cluster_name, compute_pool, env_id]):
        print("❌ Error: Missing required outputs from Terraform state")
        sys.exit(1)

    # Verify data exists in the topic
    print("⚠️  Make sure 'uv run lab3-datagen' is running in another terminal!")
    print("   (The datagen must produce data BEFORE deploying Flink pipelines)\n")

    input("Press Enter when lab3-datagen has been running for at least 1 minute...")
    print()

    for i, stmt in enumerate(STATEMENTS, 1):
        print(f"[{i}/{len(STATEMENTS)}] Submitting: {stmt['name']}...")
        cmd = [
            "confluent", "flink", "statement", "create", stmt["name"],
            "--sql", stmt["sql"].strip(),
            "--compute-pool", compute_pool,
            "--environment", env_id,
            "--database", cluster_name,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            # Check if error is "already exists" - that's okay
            if "already exists" in result.stderr.lower():
                print(f"  ℹ️  Already exists (skipping)")
            else:
                print(f"  ❌ Failed: {result.stderr.strip()}")
                sys.exit(1)
        else:
            print(f"  ✅ Submitted successfully")

        if i < len(STATEMENTS):
            time.sleep(3)  # Brief pause between statements

    print("\n✅ All Flink pipelines deployed!")
    print("\nMonitor in Confluent Cloud → Flink → Statements")
    print("or run: confluent flink statement list --environment", env_id)


if __name__ == "__main__":
    main()
