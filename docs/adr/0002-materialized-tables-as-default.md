# 0002: Use `CREATE OR ALTER MATERIALIZED TABLE` for every lab pipeline

**Status:** Accepted (2026-06-04)

## Context

The labs originally used plain `CREATE TABLE … AS SELECT …` (CTAS) for every pipeline statement. Lab 2 was migrated to `CREATE OR ALTER MATERIALIZED TABLE …` first, partly to demonstrate materialized-table syntax and partly because `OR ALTER` makes re-running the walkthrough idempotent on schema-compatible changes. Lab 1 and Lab 3 lagged behind, creating inconsistency across the repo and confusing users following multiple labs in sequence.

## Decision

Every `CREATE` statement in every walkthrough — and every `CREATE` statement in the helper script `scripts/lab3/lab3_flink.py` — uses `CREATE OR ALTER MATERIALIZED TABLE`.

Bucketing clauses are written as `DISTRIBUTED BY (partition_column) INTO N BUCKETS` (Lab 3 partitions by `tower_id`); the previous Lab 3 syntax `DISTRIBUTED INTO 6 BUCKETS` (no `BY`) is replaced with the explicit form.

Plain `CREATE TABLE` is still valid in two places:

- `terraform/lab{1,2,3}/main.tf` — for source tables fed by the Faker connector or Python datagen. Those need a fixed schema before any data arrives, so `CREATE TABLE` (not materialized) is correct.
- (Not in walkthroughs.) The guard test `test_sql_extraction.test_uses_materialized_table_consistently` fails the suite if a plain `CREATE TABLE` leaks into a walkthrough.

## Consequences

- Backing Kafka topic semantics: a materialized table writes to a Kafka topic with the same name, in upsert/changelog mode keyed by the table's primary key (or hash of the row when no key is declared). The standard Avro consumer in `scripts/tests/helpers/kafka_helper.py` reads these topics correctly — no consumer-mode change was needed.
- `OR ALTER` makes the labs forgiving: re-running a CREATE after a schema-compatible change updates the materialized table in place instead of failing with "already exists".
- Tests assume materialized-table backing topics throughout. If a future lab needs a different sink (Snowflake, BigQuery, etc.), the table semantics may differ and the assertion strategy will need to adapt.

## Notes

- Confluent Cloud Flink documents `DISTRIBUTED BY (col) INTO n BUCKETS` as the canonical form for materialized tables. The `BY` clause is what we use across the board now.
