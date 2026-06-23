# Lab 2: Real-Time Payment Fraud Detection with `ML_DETECT_ANOMALIES`

This lab demonstrates a real-time payment fraud detection pipeline using the built-in `ML_DETECT_ANOMALIES` function.

The twist: fraud isn't "a big dollar amount." A `$400` charge is obvious fraud for someone whose purchases never
top `$50`, while it's a rounding error for a high-roller who routinely spends `$4,800`. So we run **an independent
ARIMA model per customer** — each one learns that customer's *own* normal spending and flags what's anomalous **for
them**. No global threshold can do this.

[Learn more about the built-in anomaly detection functions on Confluent Cloud for Apache Flink.](https://docs.confluent.io/cloud/current/ai/builtin-functions/detect-anomalies.html)

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

If you haven't cloned the repository yet, clone it first:

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

This provisions the core Confluent Cloud environment, along with the `transactions` table — a real Kafka-backed stream
fed by a [Flink faker](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html) pipeline
that generates ~50 synthetic card transactions per second across 50 customers, split into three **spending tiers**
so no two customers look alike.

<img src="./assets/lab2/Lab2-architecture.png" alt="Lab 2 Architecture" style="max-width: 50%;" />

## Walkthrough

### Data Generation

The `transactions` topic streams ~50 records per second across 50 customers. Each row is a card transaction:
a `transaction_id`, the `customer_id`, a `customer_segment` label, the `merchant`, the `amount`, and a
`transaction_ts`.

The 50 customers fall into three spending tiers, so the *same dollar amount* means very different things depending on
who spent it:

| `customer_segment` | Customers | Normal amounts | Fraud spikes (~0.13%) |
| ------------------ | ---------------- | ----------------- | --------------------- |
| `SMALL_SPENDER`    | CUST-0001–0017   | ~$42 – $48        | ~$125 – $890          |
| `MAINSTREAM`       | CUST-0018–0034   | ~$420 – $480      | ~$1,250 – $8,895      |
| `BIG_SPENDER`      | CUST-0035–0050   | ~$4,200 – $4,800  | ~$12,500 – $88,950    |

> [!NOTE]
>
> `customer_segment` is a label **for you**, not for the model. `ML_DETECT_ANOMALIES` never sees it — each model is
> keyed only on `customer_id` and learns that customer's normal from their own transaction history. The segment is
> here purely so the results are easy to read.

To explore the source data, open a SQL workspace in the [Confluent Cloud Flink UI](https://confluent.cloud/go/flink),
select your environment and compute pool, and run the following query.

```sql no-parse
SELECT * FROM transactions LIMIT 10;
```

Example output:

| transaction_id | customer_id | customer_segment | merchant         | amount  | transaction_ts          |
| -------------- | ----------- | ---------------- | ---------------- | ------- | ----------------------- |
| 5bd84c3c-…     | CUST-0007   | SMALL_SPENDER    | Hahn-Predovic    | 44.50   | 2026-06-10 12:39:01.000 |
| 9bd56699-…     | CUST-0031   | MAINSTREAM       | Bergstrom Inc    | 460.00  | 2026-06-10 12:39:01.000 |
| c1220339-…     | CUST-0043   | BIG_SPENDER      | Kuhic and Sons   | 4452.00 | 2026-06-10 12:39:02.000 |

**See how different the customers are.** Run this to compare each customer's spending range — notice a `SMALL_SPENDER`
lives entirely under `$50`, while a `BIG_SPENDER`'s *typical* purchase dwarfs the other tier's largest:

```sql no-parse
SELECT
  customer_id,
  customer_segment,
  ROUND(MIN(amount), 2) AS min_amount,
  ROUND(MAX(amount), 2) AS max_amount,
  ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY customer_id, customer_segment
ORDER BY customer_id;
```

Example output (note `CUST-0001`'s entire range fits inside a single one of `CUST-0050`'s ordinary purchases):

| customer_id | customer_segment | min_amount | max_amount | avg_amount |
| ----------- | ---------------- | ---------- | ---------- | ---------- |
| CUST-0001   | SMALL_SPENDER    | 42.00      | 48.00      | 45.10      |
| CUST-0025   | MAINSTREAM       | 420.00     | 480.00     | 451.00     |
| CUST-0050   | BIG_SPENDER      | 4200.00    | 4800.00    | 4510.30    |

### The Approach

ARIMA — the model behind `ML_DETECT_ANOMALIES` — expects an **evenly spaced time series**: one value per timestamp
per model. Raw transactions don't look like that (each customer's payments arrive at irregular moments), so feeding
raw events directly to the model produces noisy bounds and false alarms on ordinary transactions.

The fix is a standard streaming pattern, built as **three small statements that each do one thing**:

1. **`transaction_features`** (a view) — aggregate each customer's transactions into evenly spaced **15-second tumbling
   windows**, taking the **largest** transaction in each window (`MAX(amount)`)
2. **`flagged_windows`** (a materialized table) — run one `ML_DETECT_ANOMALIES` model per customer over that windowed
   series and keep only the windows the model flags
3. **`fraud_transactions`** (a materialized table) — join the flagged windows back to `transactions` to recover the
   actual offending payment

> **Why `MAX(amount)` per window?** Fraud here is a single oversized transaction. Averaging the window dilutes the
> signal: one spike among ~15 normal charges barely moves the mean and can slip below the model's upper bound.
> Tracking the window's *largest* transaction means the model learns each customer's normal spending **ceiling** —
> any single charge that breaks through it stands out immediately.

### 1. Create the `transaction_features` View

A [view](https://docs.confluent.io/cloud/current/flink/reference/statements/create-view.html) is just a saved query —
it adds no data-bearing topic. This one turns the raw stream into one evenly spaced row per customer per 15-second
window, carrying that window's largest transaction (and the customer's segment label).

```sql
CREATE VIEW IF NOT EXISTS transaction_features AS
SELECT
  customer_id,
  customer_segment,
  window_start,
  window_end,
  window_time,
  MAX(amount) AS max_amount
FROM TUMBLE(TABLE transactions, DESCRIPTOR(transaction_ts), INTERVAL '15' SECOND)
GROUP BY customer_id, customer_segment, window_start, window_end, window_time;
```

### 2. Create the `flagged_windows` Table

Now run the model. `ML_DETECT_ANOMALIES` trains **one ARIMA model per customer** (`PARTITION BY customer_id`) over the
windowed `max_amount` series and returns a struct describing whether each point is anomalous. We keep only the windows
where the largest transaction broke above **that customer's** model bound. The result is persisted as a
[materialized table](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html).

```sql
CREATE OR ALTER MATERIALIZED TABLE flagged_windows AS
WITH scored AS (
  -- One ARIMA model per customer over its windowed max-amount series
  SELECT
    customer_id,
    customer_segment,
    window_start,
    window_end,
    max_amount,
    ML_DETECT_ANOMALIES(
      CAST(max_amount AS DOUBLE), window_time,
      JSON_OBJECT(
        'minTrainingSize'      VALUE 10,
        'maxTrainingSize'      VALUE 24,
        'confidencePercentage' VALUE 99.0,
        'enableStl'            VALUE FALSE
      )
    ) OVER (
      PARTITION BY customer_id  -- one model per customer
      ORDER BY window_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anom
  FROM transaction_features
)
-- Keep only windows the customer's own model flagged
SELECT
  customer_id,
  customer_segment,
  window_start,
  window_end,
  max_amount,
  CAST(ROUND(anom.upper_bound, 2)    AS DOUBLE) AS model_upper_bound,
  CAST(ROUND(anom.forecast_value, 2) AS DOUBLE) AS model_forecast_value
FROM scored
WHERE CAST(max_amount AS DOUBLE) > anom.upper_bound
  AND max_amount > 1.5 * anom.forecast_value;
```

`ML_DETECT_ANOMALIES` returns a struct with these fields:

| Field            | Type      | Description                                                            |
|------------------|-----------|------------------------------------------------------------------------|
| `is_anomaly`     | BOOLEAN   | `TRUE` when the value falls outside the predicted confidence interval  |
| `forecast_value` | DOUBLE    | The model's predicted value for this timestamp                         |
| `upper_bound`    | DOUBLE    | Upper edge of the model's expected range                               |
| `lower_bound`    | DOUBLE    | Lower edge of the model's expected range                               |
| `actual_value`   | DOUBLE    | The observed value the model evaluated                                 |
| `rmse`           | DOUBLE    | Rolling root-mean-square error of recent predictions                   |
| `aic`            | DOUBLE    | Akaike Information Criterion of the current model fit                  |
| `timestamp`      | TIMESTAMP | The timestamp of the evaluated point                                   |

> [!NOTE]
>
> `minTrainingSize: 10` means each model needs 10 windows (~2.5 minutes) before it starts scoring, and
> `maxTrainingSize: 24` keeps training data fresh so a past spike ages out (~6 minutes) instead of skewing
> the model's bounds. Each ARIMA model trains independently per customer — 50 concurrent models from one statement.
>
> **The `max_amount > 1.5 * model_forecast_value` clause is a *relative* severity floor**, and it's the heart of this
> lab. Instead of one global dollar cutoff (which would flag every `BIG_SPENDER`'s normal purchase and miss every
> `SMALL_SPENDER`'s fraud), it requires the spike to be at least 50% above **that customer's own** predicted level. It
> scales automatically per customer — a `$125` charge clears a small spender's bar (their level sits near `$48`),
> while a routine `$4,800` purchase stays comfortably under a high-roller's (whose level sits near `$4,800`). It
> also silences the first couple minutes of scoring, when `forecast_value` is still `NULL` and a young model's bounds
> are unstable.
>
> Expect a delay of **~3 minutes** before the first anomalies appear.

Peek at what the model is flagging — each row is one suspicious *window* (not yet a transaction):

```sql no-parse
SELECT * FROM flagged_windows;
```

### 3. Create the `fraud_transactions` Table

`flagged_windows` tells us *which 15-second window* for *which customer* contained a spike. This step joins those
windows back to `transactions` to recover the **actual offending payment** — the one whose amount equals the
window's flagged `max_amount` — with its `transaction_id` and `merchant` intact.

The result is persisted as a
[materialized table](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html) where **every row
is one fraudulent transaction**, ready for dashboards, alerting jobs, or sinks like Snowflake, BigQuery, Databricks,
or OpenSearch.

```sql
CREATE OR ALTER MATERIALIZED TABLE fraud_transactions AS
SELECT
  p.transaction_id,
  p.customer_id,
  p.customer_segment,
  p.merchant,
  p.amount,
  p.transaction_ts,
  f.model_upper_bound,
  f.model_forecast_value
FROM flagged_windows AS f
JOIN transactions AS p
  ON p.customer_id = f.customer_id
 AND p.transaction_ts >= f.window_start
 AND p.transaction_ts <  f.window_end
WHERE p.amount = f.max_amount;
```

To see the fraudulent transactions:

```sql no-parse
SELECT * FROM fraud_transactions ORDER BY amount;
```

Example output (ordered smallest-first — every row is fraud, even the `$125` one):

| transaction_id | customer_id | customer_segment | merchant                     | amount   | model_upper_bound | model_forecast_value |
|----------------|-------------|------------------|------------------------------|----------|-------------------|----------------------|
| 7ee83da1-…     | CUST-0007   | SMALL_SPENDER    | Kirlin, Ziemann and Dickens  | 125.08   | 48.52             | 47.90                |
| 560c1052-…     | CUST-0022   | MAINSTREAM       | Lakin, Kiehn and Greenfelder | 3640.15  | 481.07            | 479.87               |
| c9bab753-…     | CUST-0043   | BIG_SPENDER      | Huels LLC                    | 45107.50 | 4857.80           | 4792.00              |

🎯 **This is the payoff.** Look at the first row: a `$125.08` charge is flagged as fraud — an amount that's *invisible*
to any sensible global threshold, and that a `BIG_SPENDER` would clear a dozen times a day without a second glance.
Meanwhile `CUST-0050` routinely spends up to `$4,800` (you saw it above) and is **never** flagged. No single
`IF amount > X` rule can catch the `$125` fraud without drowning in false positives on the big spenders — but 50
per-customer models do it effortlessly, with zero hand-tuned thresholds.

### 4. 🎯 Challenge: Which Customers Are Getting Hit the Hardest? 💸

**Mission brief.** Fraud ops just pinged you on Slack: *"Which customers are bleeding the most? We need to call them
now."*

You have `fraud_transactions` streaming live. Build one materialized table named `fraud_by_customer` that answers,
per customer:

1. **How many** fraudulent transactions?
2. **How much** money is on the line?

Start from this skeleton and fill in `<YOUR_LOGIC>`:

```sql no-parse
CREATE OR ALTER MATERIALIZED TABLE fraud_by_customer AS
SELECT
  customer_id,
  customer_segment,
  <YOUR_LOGIC>
FROM fraud_transactions
GROUP BY customer_id, customer_segment;
```

<details>
<summary>Stuck? Peek at a hint</summary>

- Two aggregate functions will get you the whole way there: `COUNT(*)` and `SUM(amount)`
- Want clean 2-decimal totals? `CAST(ROUND(SUM(amount), 2) AS DOUBLE)`

</details>

### 5. 🧪 Bonus: A Different Algorithm, Same Per-Customer Idea

`ML_DETECT_ANOMALIES` uses **ARIMA**. Confluent Cloud also ships
[`ML_DETECT_ANOMALIES_ROBUST`](https://docs.confluent.io/cloud/current/ai/builtin-functions/detect-anomalies.html)
(Open Preview), which uses **Median Absolute Deviation (MAD)** instead — no `p`/`d`/`q` orders to tune, and it's
robust to outliers by design. Try it as a drop-in replacement for the scoring step, still **one model per customer**:

```sql no-parse
CREATE OR ALTER MATERIALIZED TABLE flagged_windows_robust AS
SELECT
  customer_id,
  customer_segment,
  max_amount,
  ML_DETECT_ANOMALIES_ROBUST(
    CAST(max_amount AS DOUBLE), window_time,
    JSON_OBJECT('window' VALUE 20, 'threshold' VALUE 3.0)
  ) OVER (
    PARTITION BY customer_id
    ORDER BY window_time
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS anom
FROM transaction_features;
```

**Things to notice:** because MAD uses medians rather than means, a single past spike doesn't drag the center around —
so you may find it needs **no severity floor at all** to stay quiet during warmup. Compare which customers each
detector flags, and how quickly each one settles.

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab 1](./Lab1-Walkthrough.md)
- **🧹 Cleanup**: Run `uv run destroy`
</content>
</invoke>
