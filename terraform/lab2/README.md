# Lab 2: Real-Time Payment Fraud Detection with Flink ML Functions

This lab builds a real-time payment fraud detection pipeline on Confluent Cloud using Apache Flink's `ML_DETECT_ANOMALIES` function. Synthetic payment data is generated via the Flink faker connector, and per-customer ARIMA models detect anomalous transaction amounts and unusual country codes.

## What You'll Build

```
payments_mock (faker table)
        │
        ▼
  ML_DETECT_ANOMALIES          ← per-customer ARIMA models
  (amount + country_code)
        │
        ▼
fraud_transactions (CTAS)      ← flagged payments only
```

## Architecture

| Component | Details |
|-----------|---------|
| **Data source** | Flink faker connector — 10 rows/sec synthetic payments |
| **Anomaly signals** | ~0.5% amount spike (`$8,750`), ~0.5% unusual country (`NG`) |
| **ML function** | `ML_DETECT_ANOMALIES` — online ARIMA, per `customer_id` |
| **Output** | `fraud_transactions` table (CTAS) in Confluent Flink UI |
| **Infrastructure** | Terraform — provisions the `payments_mock` source table only |

## Prerequisites

Complete the [core infrastructure setup](../core/) before running this lab. Lab 2 reads Terraform remote state from `../core/terraform.tfstate`.

## Terraform Resources

Terraform provisions a single Flink DDL statement that creates the `payments_mock` source table:

| Resource | Description |
|----------|-------------|
| `confluent_flink_statement.create_payments_mock` | Creates the `payments_mock` table backed by the faker connector |


```

## Data Schema

The `payments_mock` table generates realistic payment records with embedded fraud signals:

| Field | Type | Notes |
|-------|------|-------|
| `payment_id` | VARCHAR | UUID |
| `customer_id` | VARCHAR | 50 fixed IDs (`CUST-0001` – `CUST-0050`) |
| `merchant_id` | VARCHAR | Regex `MERCH-[0-9]{4}` |
| `merchant_name` | VARCHAR | Faker company name |
| `merchant_category` | VARCHAR | GROCERY (most common), RESTAURANT, ELECTRONICS, TRAVEL, OTHER |
| `amount` | DOUBLE | `$12.50`–`$110.75` normal; `$8,750.00` ~0.5% fraud signal |
| `currency` | VARCHAR | Always `USD` |
| `payment_method` | VARCHAR | CREDIT_CARD (60%), DEBIT_CARD (30%), WIRE_TRANSFER (10%) |
| `card_type` | VARCHAR | VISA, MASTERCARD, AMEX, DISCOVER |
| `card_last_four` | VARCHAR | 4-digit regex |
| `channel` | VARCHAR | IN_STORE, ONLINE, MOBILE_APP, ATM |
| `transaction_type` | VARCHAR | PURCHASE (80%), REFUND (10%), CASH_ADVANCE (10%) |
| `country_code` | VARCHAR | `US` (~50%), `GB` (~49.5%), `NG` ~0.5% fraud signal |
| `city` | VARCHAR | Faker city name |
| `ip_address` | VARCHAR | Faker IPv4 address |
| `transaction_ts` | TIMESTAMP(3) | Event-time; up to 5 seconds in the past; 5s watermark |

## Fraud Detection Query (Run in Confluent Flink UI)

After Terraform deploys `payments_mock`, run this query manually in the Confluent Cloud Flink UI to create the fraud output table:

```sql
CREATE TABLE fraud_transactions AS
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
      CASE WHEN country_code = 'NG' THEN 1.0 ELSE 0.0 END, transaction_ts,
      JSON_OBJECT(
        'minTrainingSize' VALUE 10,
        'confidencePercentage' VALUE 99.0,
        'enableStl' VALUE FALSE
      )
    ) OVER (
      PARTITION BY customer_id
      ORDER BY transaction_ts
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS loc_anom
  FROM payments_mock AS p
)
SELECT
  payment_id, customer_id, merchant_id, merchant_name, merchant_category,
  amount, currency, payment_method, card_type, card_last_four, channel,
  transaction_type, country_code, city, ip_address, transaction_ts,
  amount_anom.is_anomaly AS is_amount_anomaly,
  loc_anom.is_anomaly    AS is_location_anomaly
FROM with_anom
WHERE amount_anom.is_anomaly = TRUE OR loc_anom.is_anomaly = TRUE;
```

> **Note:** `minTrainingSize: 10` is set low for demo purposes so models warm up quickly. In production, use `256` or higher.

## Visualising Anomalies

Use the following query to monitor anomaly counts over time per customer:

```sql
SELECT
  window_start,
  window_end,
  customer_id,
  COUNT(*) FILTER (WHERE is_amount_anomaly)   AS amount_anomalies,
  COUNT(*) FILTER (WHERE is_location_anomaly) AS location_anomalies,
  COUNT(*) AS total_fraud_flagged
FROM TABLE(
  TUMBLE(TABLE fraud_transactions, DESCRIPTOR(transaction_ts), INTERVAL '1' MINUTE)
)
-- Optional: filter to a specific customer
-- WHERE customer_id = 'CUST-0001'
GROUP BY window_start, window_end, customer_id
ORDER BY window_start DESC, total_fraud_flagged DESC;
```

## Outputs

| Output | Description |
|--------|-------------|
| `confluent_environment_id` | Confluent environment ID (from core) |
| `confluent_kafka_cluster_id` | Kafka cluster ID (from core) |
| `confluent_flink_compute_pool_id` | Flink compute pool ID (from core) |
| `payments_mock_statement_name` | Name of the deployed Flink DDL statement |
