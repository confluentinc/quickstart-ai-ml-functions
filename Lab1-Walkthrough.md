# Lab 1: Predictive Maintenance for CNC Machines

This lab builds a real-time anomaly detection pipeline for CNC machine telemetry using `ML_DETECT_ANOMALIES` in Confluent Cloud for Apache Flink.

<img src="./assets/lab1/lab1-architecture.png" alt="Architecture Diagram" style="max-width: 50%;" />

Simulated sensor data (motor current, RPM, vibration) streams through three stages: raw telemetry from CNC machines → smoothed health features → per-machine anomaly detection. The goal is catching early signs of bearing wear or spindle failure before they cause downtime.

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

## Deploy the Demo

Clone the repository:

```bash
git clone https://github.com/confluentinc/quickstart-ai-ml-functions.git
cd quickstart-ai-ml-functions
```

Run the deployment script:

```bash
uv run deploy
```

> **Optional:** Register the project-scoped Confluent MCP server in Claude or Codex:
>
> ```bash
> uv run setup-mcp
> ```

---

## Walkthrough

### 1. Understanding the Data

The raw data pipeline is already set up — a Faker connector streams CNC machine telemetry into `cnc_machine_signals`. Explore it in the Flink SQL workspace:

```sql
SELECT * FROM cnc_machine_signals;
```

![CNC machine telemetry Data](./assets/lab1/lab1-cnc-digital-twin-results.png)

Example event:

```json
{
 "machine_id": "CNC-1",
 "motor_current": 11.33,
 "rpm": 1453.3511458792168,
 "voltage": 220,
 "vibration_raw": 0.022443748602164096,
 "ts": "..."
}
```

---

### 2. Sensor Feature Transformation

Before running anomaly detection, smooth the raw vibration signal and compute an efficiency index. Run the following in the Flink SQL workspace:

```sql
CREATE OR ALTER MATERIALIZED TABLE machine_health_features AS
SELECT
    machine_id,
    ts,
    vibration_raw,
    -- Smoothing: Average of the last 10 rows
    AVG(vibration_raw) OVER (
        PARTITION BY machine_id
        ORDER BY ts
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) AS vibration_smoothed,
    (rpm / NULLIF(motor_current, 0)) AS efficiency_index
FROM cnc_machine_signals;
```

---

### 3. Detect Machine Anomalies

Run the following in the Flink SQL workspace. `ML_DETECT_ANOMALIES` trains an independent ARIMA model per machine and flags rows where smoothed vibration falls outside the predicted confidence interval.

```sql
CREATE OR ALTER MATERIALIZED TABLE equipment_anomalies AS
SELECT
    machine_id,
    ts,
    vibration_smoothed,
    anomaly.is_anomaly,
    anomaly.forecast_value,
    anomaly.lower_bound,
    anomaly.upper_bound
FROM (
    SELECT
        machine_id,
        ts,
        vibration_raw,
        vibration_smoothed,
        ML_DETECT_ANOMALIES(
            vibration_smoothed,
            ts,
            JSON_OBJECT(
                'minTrainingSize'      VALUE 50,
                'maxTrainingSize'      VALUE 300,
                'confidencePercentage' VALUE 99.0
            )
        ) OVER (
            PARTITION BY machine_id
            ORDER BY ts
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS anomaly
    FROM machine_health_features
)
WHERE anomaly.is_anomaly = TRUE
  AND vibration_smoothed > anomaly.upper_bound;
```

![Anomaly Detection](./assets/lab1/lab1-anomaly-chart.png)

Query the results:

```sql
SELECT * FROM equipment_anomalies;
```

| Machine | Timestamp | Vibration | Is Anomaly | Forecast | Lower Bound | Upper Bound |
| ------- | --------- | --------- | ---------- | -------- | ----------- | ----------- |
| CNC-101 | 12:41:02  | 0.87      | true       | 0.021    | 0.010       | 0.035       |
| CNC-103 | 12:45:59  | 0.91      | true       | 0.019    | 0.008       | 0.032       |

Anomalies can indicate bearing wear, spindle imbalance, or tool misalignment.

---

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **→ Next Lab**: [Lab 2](./Lab2-Walkthrough.md)
- **🧹 Cleanup**: Run `uv run destroy`
