# Lab 3: Proactive Network Capacity Planning with ML_FORECAST

Real-time network capacity planning that predicts per-tower throughput and alerts before congestion impacts customers.

Built on **Confluent Cloud for Apache Flink** with **built-in ML time-series forecasting** — each tower maintains its own independent ARIMA model.

[Learn more about ML_FORECAST on Confluent Cloud](https://docs.confluent.io/cloud/current/ai/builtin-functions/forecast.html)

---

## Business Context

**TowerNet Wireless** operates 10 cell towers across 3 regions (NYC, Chicago, LA), each handling traffic that swings dramatically between 2 AM troughs (~10% utilization) and 6 PM evening peaks (~90% utilization).

**The Problem:** Congestion is detected **reactively** — operations engineers see degraded throughput in dashboards and scramble to reroute traffic. By then, customers have already experienced dropped calls and slow data.

**The Solution:** Shift from reactive to **proactive** — predict when a tower will exceed 85% capacity and surface actionable alerts with enough lead time for automated or manual remediation.

---

## Architecture Overview

```
Python datagen (uv run lab3-datagen)
        │
        ▼  Avro/Schema Registry, keyed by tower_id
lab3_tower_traffic  (raw telemetry, 6 partitions)
        │
        ▼  10-second TUMBLE window, GROUP BY tower_id
lab3_tower_agg      (windowed averages, 6 partitions)
        │
        ▼  ML_FORECAST PARTITION BY tower_id (ARIMA per tower, horizon=6)
lab3_forecasts      (predicted throughput + confidence bands, 6 partitions)
        │
        ▼  WHERE forecast > 85% capacity
lab3_capacity_alerts  (actionable ops alerts, 6 partitions)
```

| Topic                  | Purpose                                          | Key        | Partitions |
|------------------------|--------------------------------------------------|------------|------------|
| `lab3_tower_traffic`   | Raw tower metrics from Python datagen            | `tower_id` | 6          |
| `lab3_tower_agg`       | 10-second tumble-windowed averages per tower     | `tower_id` | 6          |
| `lab3_forecasts`       | ML_FORECAST output with predictions              | `tower_id` | 6          |
| `lab3_capacity_alerts` | Alerts when forecasted traffic exceeds threshold | `tower_id` | 6          |

---

## Prerequisites

**Required accounts & credentials:**

[![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/)

<details>
<summary>Install the Prerequisities (Mac/Windows)</summary>

**Mac:**

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

</details>

---

## Quick Start

**1. Clone and navigate:**

```bash
git clone https://github.com/confluentinc/quickstart-ai-ml-functions.git
cd quickstart-ai-ml-functions
```

**2. Deploy Lab 3:**

```bash
uv run deploy
```

> **Optional:** Register the project-scoped Confluent MCP server in Claude or Codex:
>
> ```bash
> uv run setup-mcp
> ```

This provisions:

- Shared Confluent Cloud environment, Kafka cluster, and Flink compute pool
- The `lab3_tower_traffic` source Kafka topic and matching Flink table (Avro / Schema Registry, 6 partitions)

At this point Confluent Cloud → Flink → Statements shows only the source-table statement. The downstream CTAS pipelines are submitted in the *Deploy Flink pipelines* step below.

The downstream topics (`lab3_tower_agg`, `lab3_forecasts`, `lab3_capacity_alerts`) and their Flink pipelines are created by `uv run lab3-flink` in the *Deploy Flink pipelines* step below — **after** the datagen has produced data. Letting the CTAS create those topics ensures the inferred column schema is attached correctly.

**3. Start the data generator:**

In a separate terminal:

```bash
uv run lab3-datagen
```

On startup, the generator **burst-produces 60 minutes of backdated records** (600 messages across 10 towers, oldest-first) so each tower's ARIMA model has training history immediately:

```
[backfill] Producing 60 steps × 10 towers = 600 records (oldest-first)...
[backfill] Done — 600 records published
[backfill] Switching to live mode...
Publishing to 'lab3_tower_traffic' — 10 towers — Ctrl+C to stop
```

**4. Deploy Flink pipelines** (after datagen runs for 1+ minute):

```bash
uv run lab3-flink
```

`lab3_forecasts` will start emitting rows within ~2 minutes.

**Generator options:**

```bash
uv run lab3-datagen --scenario peak         # start at 17:00 UTC — alerts fire reliably (recommended for demos)
uv run lab3-datagen --scenario cycle        # compress 24h into ~6 min — see the full traffic curve live
uv run lab3-datagen --backfill-minutes 90   # more history
uv run lab3-datagen --no-backfill           # skip backfill
uv run lab3-datagen --interval 0.5          # faster live cadence (2x per second)
uv run lab3-datagen --verbose               # per-message delivery logs
```

**Traffic scenarios (`--scenario`):**

The generator's traffic shape is a daily curve with a morning peak (~08:00 UTC, ~85% utilization) and an evening peak (~18:30 UTC, ~98% utilization), with a deep trough overnight (~18%). The `--scenario` flag controls which slice of that curve you see:

| Scenario             | What it does                                                                                                                                                                | When to use                                                                                                                                                   |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `realtime` (default) | Uses wall-clock UTC time. The traffic shape advances naturally.                                                                                                             | Long-running deployments; when you want to observe real diurnal behavior. **Outside ~07-09 and ~17-20 UTC, alerts will be empty — this is correct behavior.** |
| `peak`               | Starts simulated time at 17:00 UTC and advances at 0.25× wall-clock — slow enough that a long demo stays near 17:00 instead of drifting into the 18:30 natural peak where the curve clips at 100% and ARIMA forecasts blow up. Underlying curve sits at ~87% utilization; ±12% per-reading jitter gives a typical range of 75-99%, so most windows breach the 85% alert threshold. TUMBLE windows close every ~40 wall-seconds. | Live demos where you need the alert pipeline to fire reliably within ~2 minutes regardless of when you're running.                                            |
| `cycle`              | Compresses 24 simulated hours into ~6 wall-clock minutes. Live mode cycles through the full curve repeatedly.                                                               | Learning/teaching — the most interesting story because ARIMA visibly tracks the trough → peak transition and forecasts ahead of the breach.                   |

---

## Demo Walkthrough

Open a SQL workspace in the [Confluent Cloud Flink UI](https://confluent.cloud/go/flink), select your environment and compute pool, then follow along:

---

### 1. Verify Incoming Data

Confirm the Python generator is producing to `lab3_tower_traffic`:

```sql
SELECT tower_id, region, throughput_mbps, capacity_mbps, `ts_ms`
FROM lab3_tower_traffic
LIMIT 20;
```

You should see all 10 towers streaming in, with `throughput_mbps` values that reflect time of day (low at night, peaking in morning/evening).

---

### 2. Explore the 10-Second Aggregation

The `lab3_tower_agg` pipeline computes per-tower windowed statistics. This CTAS is submitted by `uv run lab3-flink` — the SQL below is for reference; you do not need to run it manually.

```sql
CREATE OR ALTER MATERIALIZED TABLE lab3_tower_agg
DISTRIBUTED BY (tower_id) INTO 6 BUCKETS
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
  TUMBLE(TABLE lab3_tower_traffic, DESCRIPTOR(`$rowtime`), INTERVAL '10' SECONDS)
)
GROUP BY tower_id, region, window_start, window_end;
```

**Query the aggregates:**

```sql
SELECT
  tower_id,
  region,
  window_start,
  window_end,
  ROUND(avg_throughput_mbps, 1)  AS avg_mbps,
  ROUND(max_throughput_mbps, 1)  AS peak_mbps,
  ROUND(capacity_mbps, 0)        AS capacity_mbps,
  ROUND((avg_throughput_mbps / capacity_mbps) * 100, 1) AS utilization_pct
FROM lab3_tower_agg
ORDER BY window_end DESC, tower_id
LIMIT 30;
```

Each row represents one 10-second window for one tower. `utilization_pct` shows current tower load.

---

### 3. The Key Query: ML_FORECAST with PARTITION BY tower_id

This is the heart of the lab. **ML_FORECAST with `PARTITION BY tower_id` trains a separate ARIMA model for each of the 10 towers** — no offline training, no model registry, no Python ML framework required.

This CTAS is submitted by `uv run lab3-flink` — the SQL below is for reference; you do not need to run it manually.

```sql
CREATE OR ALTER MATERIALIZED TABLE lab3_forecasts
DISTRIBUTED BY (tower_id) INTO 6 BUCKETS
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
  FROM lab3_tower_agg
) f
WHERE CARDINALITY(forecast) >= 1;
```

**Why this matters:**

| Parameter / pattern           | Value       | Meaning                                                                                 |
|-------------------------------|-------------|-----------------------------------------------------------------------------------------|
| `PARTITION BY tower_id`       | per-tower   | Each tower's ARIMA model is completely independent — NYC traffic never contaminates LA. |
| `horizon = 6`                 | 6 windows   | Predicts 6×10s ≈ **60 seconds ahead** in this demo (10-second windows).                 |
| `minTrainingSize = 10`        | 10 windows  | Model activates after ~10 windows of data (~100 seconds with 10-second windows).        |
| default `UNBOUNDED PRECEDING` | all history | Model can use the full history per tower; ARIMA's sliding training window is internal.  |

**Query the results:**

```sql
SELECT
  tower_id,
  region,
  ROUND(avg_throughput_mbps, 1)      AS current_mbps,
  ROUND(forecast_throughput_mbps, 1) AS forecast_mbps,
  ROUND(confidence_lower_mbps, 1)    AS lower_bound,
  ROUND(confidence_upper_mbps, 1)    AS upper_bound,
  ROUND(capacity_mbps, 0)            AS capacity_mbps,
  ROUND((forecast_throughput_mbps / capacity_mbps) * 100, 1)
    AS forecast_utilization_pct
FROM lab3_forecasts
ORDER BY forecast_utilization_pct DESC
LIMIT 20;
```

---

### 4. Capacity Alerts — Proactive Ops

The `lab3_capacity_alerts` pipeline filters the forecast stream and emits a structured alert whenever any tower is predicted to exceed **85% capacity within the next forecast step (~1 minute in this demo)**. This CTAS is submitted by `uv run lab3-flink` — the SQL below is for reference; you do not need to run it manually.

```sql
CREATE OR ALTER MATERIALIZED TABLE lab3_capacity_alerts
DISTRIBUTED BY (tower_id) INTO 6 BUCKETS
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
FROM lab3_forecasts lf
WHERE lf.forecast_throughput_mbps IS NOT NULL
  AND lf.forecast_throughput_mbps > (lf.capacity_mbps * 0.85);
```

**Query alerts:**

```sql
SELECT
  tower_id,
  region,
  ROUND(forecast_throughput_mbps, 1)  AS forecast_mbps,
  ROUND(capacity_mbps, 0)             AS capacity_mbps,
  forecast_utilization_pct,
  alert_message
FROM lab3_capacity_alerts
ORDER BY forecast_utilization_pct DESC
LIMIT 20;
```

During the "evening traffic peak" period of your demo, you should see several towers breach the 85% threshold and emit alerts. During quieter periods, the alert topic will be empty — which is exactly the right behavior.

> **Not seeing any alerts?** With the default `realtime` scenario, alerts only fire during the morning (~07-09 UTC) and evening (~17-20 UTC) peaks. If you're running outside those windows, restart the generator with `uv run lab3-datagen --scenario peak` (always at peak) or `--scenario cycle` (24h compressed into ~6 minutes) — see [Traffic scenarios](#traffic-scenarios---scenario) above.

> **Why does `forecast_utilization_pct` sometimes exceed 100%?** ARIMA is a statistical forecaster — it predicts where the signal is *heading*, not where capacity allows it to go. When recent throughput is at or near capacity and the model detects upward drift, it projects values like 102-110% as the "demanded" throughput. In real network ops this is the most useful kind of alert: predicted demand exceeds carriage, so add capacity (or shed traffic) before the breach. The `peak` scenario is pinned to 17:00 UTC specifically to keep observations below the 100% clipping ceiling so ARIMA isn't trained on a truncated distribution — that's what produced the absurd 130%+ forecasts you'd otherwise see.

---

### 5. Discussion Points for the Demo

**Why PARTITION BY matters**

Without `PARTITION BY tower_id`, `ML_FORECAST` would train **one global model** on all towers' data mixed together — which makes no sense for capacity planning. A busy NYC macro site and a quiet rural tower have completely different traffic shapes. `PARTITION BY tower_id` ensures **each tower gets its own ARIMA model** based only on its own history.

**Confidence bands as a risk signal**

`confidence_lower_mbps` and `confidence_upper_mbps` come from the ARIMA model's predictive distribution. A **wide band** near capacity tells a different story than a narrow one:

- If the **lower bound** is already above 85%, you have high-confidence risk.
- If only the **upper bound** is above 85%, it's a lower-confidence "heads-up".

This is a good hook to talk about risk-based alerting, not just point estimates.

**Lead time vs. horizon trade-off**

In the demo:

- Window size: **10 seconds**
- `horizon = 6` → about **60 seconds of lead time**

In a production setup with 5-minute windows:

- `horizon = 6` → **30 minutes** of lead time
- `horizon = 12` → **60 minutes**, but with lower short-term accuracy

The "right" horizon depends on how quickly ops can actually respond (ticket → engineer → action).

**Scaling to thousands of towers**

Because each ARIMA model is scoped by `PARTITION BY tower_id`, adding more towers is a **linear scale-out problem**:

- No re-training step when you onboard a tower.
- No separate model registry to manage.
- The Flink compute pool simply runs more partitions in parallel.

This is a nice contrast to a traditional batch ML workflow that would require periodic retraining and deployment for every tower.

---

## Cleanup

```bash
uv run destroy
```

This removes all Confluent Cloud resources provisioned for Lab 3.

---

## Sign up for early access to Flink AI features

For early access to exciting new Flink AI features, [fill out this form and we'll add you to our early access previews.](https://events.confluent.io/early-access-flink-features)
