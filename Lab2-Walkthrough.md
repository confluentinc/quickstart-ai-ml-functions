# Lab 2: Real-Time Payment Fraud Detection with `ML_DETECT_ANOMALIES`

This lab demonstrates a real-time payment fraud detection pipeline using the built-in `ML_DETECT_ANOMALIES` function. Per-customer ARIMA models flag unusually large transaction amounts, and unusual cash advance activity.

[Learn more about the built-in anomaly detection functions on Confluent Cloud for Apache Flink.](https://docs.confluent.io/cloud/current/ai/builtin-functions/detect-anomalies.html)

## Deploy the Demo

```bash
uv run deploy lab2
```

This provisions the core Confluent Cloud environment, along with the `payments` source table, which uses the [Flink faker connector](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html)generating ~10 synthetic payment records per second across 50 customers.

## Walkthrough

### Data Generation

The `payments` topic uses the [Flink faker connector](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html) to generate 10 payment records per second across 50 customers. The synthetic data stream contains two financial fraud signals, which we aim to detect with `ML_DETECT_ANOMALIES`:

- **Transaction size spikes:** ~0.5% of transactions have an amount of `$8,750` (vs. a normal range of `$12.50`–`$110.75`)
- **Cash advance spikes:** `CASH_ADVANCE` transaction type appears at ~10% baseline; per-customer spikes above that baseline over a rolling time window are flagged

### 1. Create the `payments_flagged` Table

Open a SQL workspace in the [Confluent Cloud Flink UI](https://confluent.cloud/go/flink), select your environment and compute pool, and run the following query.

Two `ML_DETECT_ANOMALY` models run per customer — one on transaction amount, one on transaction type. Anytime either model detects fraud, it emits an anomaly event to the`payments_flagged` topic.

```sql
CREATE TABLE payments_flagged AS
WITH with_anom AS (
  SELECT
    p.*,
    
    ML_DETECT_ANOMALIES(
      CAST(amount AS DOUBLE), transaction_ts,
      JSON_OBJECT(
        'minTrainingSize' VALUE 10,
        'confidencePercentage' VALUE 99.0,
        'enableStl' VALUE FALSE
      )
    ) OVER (
      PARTITION BY customer_id
      ORDER BY transaction_ts
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS amount_anom,
    
    ML_DETECT_ANOMALIES(
      CASE WHEN transaction_type = 'CASH_ADVANCE' THEN 1.0 ELSE 0.0 END, transaction_ts,
      JSON_OBJECT(
        'minTrainingSize' VALUE 10,
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
  COALESCE(amount_anom.is_anomaly, FALSE) AS is_amount_anomaly,
  COALESCE(cash_anom.is_anomaly, FALSE)   AS is_cash_advance_anomaly
FROM with_anom
WHERE amount_anom.is_anomaly IS TRUE OR cash_anom.is_anomaly IS TRUE;
```

> [!NOTE]
>
> `minTrainingSize: 10` is set low so models warm up quickly for demo purposes. Each ARIMA model trains independently per customer — expect a short delay before the first anomalies appear.

To see the fraud detection anomalies, run:

```sql
SELECT * FROM payments_flagged;
```

<img src="./assets/lab2/anomalies.png" alt="Fraud transactions results" width="100%" />

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab 1](./Lab1-Walkthrough.md)
- **🧹 Cleanup**: Run `terraform destroy` 
