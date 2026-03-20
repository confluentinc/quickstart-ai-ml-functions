# Lab 2: Real-Time Payment Fraud Detection with `ML_DETECT_ANOMALIES`

This lab demonstrates a real-time payment fraud detection pipeline using the built-in `ML_DETECT_ANOMALIES` function.

A single SQL statement runs **two independent ARIMA models per customer** simultaneously — one on transaction amount, one
on cash advance frequency — flagging unusual activity the moment it appears in the stream.

[Learn more about the built-in anomaly detection functions on Confluent Cloud for Apache Flink.](https://docs.confluent.io/cloud/current/ai/builtin-functions/detect-anomalies.html)

## Deploy the Demo

```bash
uv run deploy lab2
```

This provisions the core Confluent Cloud environment, along with the `payments` source table, which uses the [Flink faker connector](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html) to generate ~10 synthetic payment records per second across 50 customers.

<img src="./assets/lab2/Lab2-architecture.png" alt="Lab 2 Architecture" style="max-width: 50%;" />

## Walkthrough

### Data Generation

The `payments` topic streams ~10 payment records per second across 50 customers. It contains two fraud signals that `ML_DETECT_ANOMALIES` is designed to catch:

- **Transaction size spikes:** ~0.5% of transactions have an amount of `$8,750` (vs. a normal range of `$12.50`–
  `$110.75`)
- **Cash advance spikes:** `CASH_ADVANCE` transaction type appears at ~10% average baseline; per-customer bursts above that
  baseline are flagged

To explore the source data:

```sql
SELECT * FROM payments LIMIT 10;
```

Example output:

| transaction_id | customer_id | amount  | transaction_type | transaction_ts          |
| -------------- | ----------- | ------- | ---------------- | ----------------------- |
| txn-00042      | cust-017    | 34.50   | PURCHASE         | 2026-04-01 12:39:01.000 |
| txn-00043      | cust-031    | 8750.00 | PURCHASE         | 2026-04-01 12:39:01.000 |
| txn-00044      | cust-017    | 22.75   | CASH_ADVANCE     | 2026-04-01 12:39:02.000 |
| txn-00045      | cust-008    | 67.10   | PURCHASE         | 2026-04-01 12:39:02.000 |

### 1. Create the `payments_flagged` Table

Open a SQL workspace in the [Confluent Cloud Flink UI](https://confluent.cloud/go/flink), select your environment and compute pool, and run the following query.

Two `ML_DETECT_ANOMALIES` models run **per customer** — one on transaction amount, one on cash advance frequency. Any
transaction where either model fires an anomaly is emitted to `payments_flagged`.

```sql
CREATE TABLE payments_flagged AS
WITH with_anom AS (
  SELECT
    p.*,

    -- Model 1: flag unusually large transaction amounts per customer
    ML_DETECT_ANOMALIES(
      CAST(amount AS DOUBLE), transaction_ts,
      JSON_OBJECT(
        'minTrainingSize'      VALUE 10,
        'confidencePercentage' VALUE 99.0,
        'enableStl' VALUE FALSE
      )
    ) OVER (
      PARTITION BY customer_id  -- one model per customer
      ORDER BY transaction_ts
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS amount_anom,

    -- Model 2: flag abnormal cash advance frequency per customer
    ML_DETECT_ANOMALIES(
      CASE WHEN transaction_type = 'CASH_ADVANCE' THEN 1.0 ELSE 0.0 END, transaction_ts,
      JSON_OBJECT(
        'minTrainingSize'      VALUE 10,
        'confidencePercentage' VALUE 99.0,
        'enableStl' VALUE FALSE
      )
    ) OVER (
      PARTITION BY customer_id
      ORDER BY transaction_ts
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cash_anom

  FROM payments AS p
)
SELECT
  p.*,
  COALESCE(CAST(p.amount AS DOUBLE) > p.amount_anom.upper_bound, FALSE) AS is_amount_anomaly,
  COALESCE(
    (CASE WHEN p.transaction_type = 'CASH_ADVANCE' THEN 1.0 ELSE 0.0 END) > p.cash_anom.upper_bound,
    FALSE
  ) AS is_cash_advance_anomaly
FROM with_anom AS p
WHERE CAST(p.amount AS DOUBLE) > p.amount_anom.upper_bound
   OR (CASE WHEN p.transaction_type = 'CASH_ADVANCE' THEN 1.0 ELSE 0.0 END) > p.cash_anom.upper_bound;
```

`ML_DETECT_ANOMALIES` returns a struct with these fields:

| Field         | Type    | Description                                                           |
|---------------|---------|-----------------------------------------------------------------------|
| `is_anomaly`  | BOOLEAN | `TRUE` when the value falls outside the predicted confidence interval |
| `upper_bound` | DOUBLE  | Upper edge of the model's expected range                              |
| `lower_bound` | DOUBLE  | Lower edge of the model's expected range                              |
| `score`       | DOUBLE  | Normalized anomaly score (higher = more anomalous)                    |

The WHERE clause filters to only rows where at least one model detected an anomaly. The SELECT adds boolean columns so
downstream consumers can tell which signal triggered the alert.

> [!NOTE]
>
> `minTrainingSize: 10` is set low so models warm up quickly for demo purposes. Each ARIMA model trains independently
> per customer — with 50 customers that's 100 concurrent models from a single SQL statement. Expect a short delay before
> the first anomalies appear.

To see the fraud detection results:

```sql
SELECT * FROM payments_flagged;
```

Example output:

| transaction_id | customer_id | amount  | transaction_type | is_amount_anomaly | is_cash_advance_anomaly |
|----------------|-------------|---------|------------------|-------------------|-------------------------|
| txn-00043      | cust-031    | 8750.00 | PURCHASE         | TRUE              | FALSE                   |
| txn-00107      | cust-014    | 22.50   | CASH_ADVANCE     | FALSE             | TRUE                    |
| txn-00219      | cust-031    | 8750.00 | PURCHASE         | TRUE              | FALSE                   |
| txn-00334      | cust-008    | 41.25   | CASH_ADVANCE     | FALSE             | TRUE                    |

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab 1](./LAB1-Walkthrough.md)
- **🧹 Cleanup**: Run `uv run destroy`
