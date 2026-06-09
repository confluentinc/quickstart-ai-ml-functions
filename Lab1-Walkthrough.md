# Lab 1: Predictive Maintenance for CNC Machines

This lab builds a real-time anomaly detection pipeline for CNC machine telemetry using `ML_DETECT_ANOMALIES` in Confluent Cloud for Apache Flink.

<img src="./assets/lab1/lab1-architecture.png" alt="Architecture Diagram" style="max-width: 50%;" />

Simulated sensor data (motor current, RPM, vibration) streams from CNC machines directly into anomaly detection. The goal is catching early signs of bearing wear or spindle failure before they cause downtime.

---

## Deploy the Demo

Clone the repository:

```bash
git clone https://github.com/confluentinc/quickstart-ai-ml-functions.git
cd quickstart-ai-ml-functions
```

Run the deployment script:

```bash
uv run deploy lab1
```

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

### 2. Detect Machine Anomalies

Run the following in the Flink SQL workspace. `ML_DETECT_ANOMALIES` trains an independent ARIMA model per machine and flags rows where vibration falls outside the predicted confidence interval.

```sql
CREATE OR ALTER MATERIALIZED TABLE equipment_anomalies AS
SELECT
    machine_id,
    ts,
    vibration_raw,
    anomaly.is_anomaly,
    anomaly.forecast_value,
    anomaly.lower_bound,
    anomaly.upper_bound
FROM (
    SELECT
        machine_id,
        ts,
        vibration_raw,
        ML_DETECT_ANOMALIES(
            vibration_raw,
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
    FROM cnc_machine_signals
)
WHERE anomaly.is_anomaly = TRUE
  AND vibration_raw > anomaly.upper_bound;
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
