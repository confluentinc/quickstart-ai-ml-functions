# Lab 3: Proactive Network Capacity Planning with `ML_FORECAST`

This lab demonstrates a **real-time network capacity planning pipeline** that predicts per-tower throughput 30 minutes into the future, enabling operations teams to take action before congestion ever reaches a customer.

Built on **Confluent Cloud for Apache Flink**, the system ingests live tower telemetry, aggregates it into 5-minute windows, and applies **built-in ML time-series forecasting** — with each tower maintaining its own independent ARIMA model.

[Learn more about the built-in ML_FORECAST function on Confluent Cloud for Apache Flink.](https://docs.confluent.io/cloud/current/ai/builtin-functions/forecast.html)

---

# Business Context

**TowerNet Wireless** operates thousands of cell towers across the US, each handling traffic that swings dramatically between 2 AM troughs (~10% utilization) and 6 PM evening peaks (~90% utilization).

Today, congestion is detected **reactively** — an operations engineer sees degraded throughput in a dashboard and scrambles to reroute traffic or spin up capacity. By then, customers have already experienced dropped calls and slow data.

The goal is to shift from reactive to **proactive**: predict when a tower will exceed 85% capacity and surface an actionable alert 30 minutes in advance, leaving time for automated or manual remediation.

---

# The Use Case: Per-Tower Throughput Forecasting

The system monitors **10 towers across 3 regions** (NYC, Chicago, LA):

- Each tower has a fixed capacity ceiling (600–1200 Mbps depending on hardware)
- Raw telemetry arrives every second from the Python data generator
- 5-minute tumble windows smooth the signal for forecasting
- `ML_FORECAST` runs **PARTITION BY tower_id** — each tower's ARIMA model trains independently on its own traffic pattern
- When any tower's forecasted throughput exceeds **85% of capacity**, an alert fires with ~30 minutes of lead time

---

# Architecture Overview

```
Python datagen (uv run lab3-datagen)
        │
        ▼  JSON, keyed by tower_id
lab3_tower_traffic  (raw telemetry, 6 partitions)
        │
        ▼  5-min TUMBLE window, GROUP BY tower_id
lab3_tower_agg      (windowed averages, 6 partitions)
        │
        ▼  ML_FORECAST PARTITION BY tower_id (ARIMA per tower, horizon=6 windows)
lab3_forecasts      (predicted throughput + confidence bands, 6 partitions)
        │
        ▼  WHERE forecast > 85% capacity
lab3_capacity_alerts  (actionable ops alerts, 6 partitions)
```

| Topic | Purpose | Key | Partitions |
|---|---|---|---|
| `lab3_tower_traffic` | Raw tower metrics from Python datagen | `tower_id` | 6 |
| `lab3_tower_agg` | 5-min tumble-windowed averages per tower | `tower_id` | 6 |
| `lab3_forecasts` | ML_FORECAST output with predictions | `tower_id` | 6 |
| `lab3_capacity_alerts` | Alerts when forecasted traffic exceeds threshold | `tower_id` | 6 |

---

# Prerequisites

Install the following tools:

**MacOS**

```bash
brew install uv git python
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
brew install --cask confluent-cli
```

**Windows**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

---

# Deploy the Demo

```bash
uv run deploy
```

Select **Lab 3** when prompted. This provisions:
- The shared Confluent Cloud environment, Kafka cluster, and Flink compute pool (core)
- 4 Kafka topics with 6 partitions each
- The `lab3_tower_traffic` source table (JSON format, reads from the Python datagen)
- The aggregation, forecasting, and alerting Flink pipelines

---

# Start the Data Generator

In a separate terminal, run:

```bash
uv run lab3-datagen
```

On startup the generator **burst-produces 60 minutes of backdated records** (600 msgs across 10 towers, oldest-first) so each tower's ARIMA model has enough training history the moment Flink processes the backfill — no waiting. You'll see:

```
[backfill] Producing 60 min × 10 towers = 600 records (oldest-first)...
[backfill] Done — 600 records published
[backfill] Switching to live mode...
Publishing to 'lab3_tower_traffic' — 10 towers — Ctrl+C to stop
```

`lab3_forecasts` will start emitting rows within ~2 minutes (time for Flink to process the backfill through the aggregation pipeline).

Options:

```bash
uv run lab3-datagen --backfill-minutes 90   # more history for slower workshops
uv run lab3-datagen --no-backfill           # skip backfill, live mode only
uv run lab3-datagen --interval 0.5          # faster live cadence (2x per second)
uv run lab3-datagen --verbose               # per-message delivery logs
```

---

# Walkthrough

Open a SQL workspace in the [Confluent Cloud Flink UI](https://confluent.cloud/go/flink), select your environment and compute pool, then follow the steps below.

---

## 1. Verify Incoming Data

Confirm the Python generator is producing to `lab3_tower_traffic`:

```sql
SELECT tower_id, region, throughput_mbps, capacity_mbps, ts
FROM lab3_tower_traffic
LIMIT 20;
```

You should see all 10 towers streaming in, with `throughput_mbps` values that reflect the time of day (low at night, peaking in the morning and evening).

---

## 2. Explore the 5-Minute Aggregation

The `lab3_tower_agg` pipeline (deployed by Terraform) computes per-tower windowed stats:

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

Each row is one 5-minute bucket for one tower. `utilization_pct` tells you how loaded each tower is right now.

---

## 3. The Key Query: ML_FORECAST with PARTITION BY tower_id

This is the heart of the lab. `ML_FORECAST` with `PARTITION BY tower_id` trains a separate ARIMA model for each of the 10 towers — no offline training, no model registry, no Python ML framework required.

```sql
CREATE TABLE lab3_forecasts
WITH ('kafka.partitions' = '6')
AS
WITH forecasted AS (
  SELECT
    tower_id,
    region,
    window_end,
    capacity_mbps,
    avg_throughput_mbps,
    ML_FORECAST(
      avg_throughput_mbps,
      window_end,
      JSON_OBJECT(
        'horizon'         VALUE 6,
        'minTrainingSize' VALUE 10
      )
    ) OVER (
      PARTITION BY tower_id
      ORDER BY window_end
      ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS fc
  FROM lab3_tower_agg
)
SELECT
  tower_id,
  region,
  window_end                 AS ts,
  capacity_mbps,
  avg_throughput_mbps,
  fc.forecast                AS forecast_throughput_mbps,
  fc.confidenceLower         AS confidence_lower_mbps,
  fc.confidenceUpper         AS confidence_upper_mbps
FROM forecasted;
```

**Why this matters:**

| Parameter | Value | Meaning |
|---|---|---|
| `PARTITION BY tower_id` | per-tower | Each tower's ARIMA model is completely independent — a congested NYC tower doesn't affect LA |
| `horizon = 6` | 6 windows | Predicts 6 × 5 min = **30 minutes ahead** |
| `minTrainingSize = 10` | 10 windows | Model activates after ~50 min of data (~10 aggregated windows) |
| `ROWS BETWEEN 23 PRECEDING AND CURRENT ROW` | 24 rows | Each model trains on the last 2 hours of windowed history |

Query the results:

```sql
SELECT
  tower_id,
  region,
  ROUND(avg_throughput_mbps, 1)       AS current_mbps,
  ROUND(forecast_throughput_mbps, 1)  AS forecast_mbps,
  ROUND(confidence_lower_mbps, 1)     AS lower_bound,
  ROUND(confidence_upper_mbps, 1)     AS upper_bound,
  ROUND(capacity_mbps, 0)             AS capacity_mbps,
  ROUND((forecast_throughput_mbps / capacity_mbps) * 100, 1) AS forecast_utilization_pct
FROM lab3_forecasts
ORDER BY forecast_utilization_pct DESC
LIMIT 20;
```

---

## 4. Capacity Alerts — Proactive Ops

The `lab3_capacity_alerts` pipeline filters the forecast stream and emits a structured alert whenever any tower is predicted to exceed 85% capacity within 30 minutes:

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

During the evening traffic peak (5–8 PM), you should see several towers breach the 85% threshold and emit alerts. During overnight hours, the alert topic will be empty — which is exactly the right behavior.

---

## 5. Discussion Points for the Demo

**Why PARTITION BY matters:**

Without `PARTITION BY tower_id`, ML_FORECAST would train a single model on all towers' data mixed together — which makes no sense for capacity planning. A busy NYC tower and a quiet rural tower have completely different traffic shapes. `PARTITION BY` ensures each tower gets a model that reflects its own unique pattern.

**Confidence bands as a risk signal:**

`confidenceLower` and `confidenceUpper` represent the ARIMA model's uncertainty. A wide confidence interval on a tower near capacity is a stronger signal to act on than a narrow one: even the lower bound of the forecast may already be above the 85% threshold.

**Lead time vs. horizon trade-off:**

`horizon = 6` gives 30 minutes of lead time with 5-minute windows. Increasing to `horizon = 12` extends lead time to 60 minutes but reduces short-term accuracy. The right value depends on how quickly your team can respond.

**Scaling to thousands of towers:**

Because each ARIMA model runs independently, adding more towers is a linear scale-out — no re-training, no model management overhead. The Flink compute pool absorbs additional parallelism automatically.

---

# Tear Down

```bash
uv run destroy
```

This removes all Confluent Cloud resources provisioned for Lab 3.
