data "terraform_remote_state" "core" {
  backend = "local"
  config  = { path = "../core/terraform.tfstate" }
}

locals {
  environment_id      = data.terraform_remote_state.core.outputs.confluent_environment_id
  compute_pool_id     = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  organization_id     = data.terraform_remote_state.core.outputs.confluent_organization_id
  service_account_id  = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  flink_rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  flink_api_key       = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
  flink_api_secret    = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  random_id           = data.terraform_remote_state.core.outputs.random_id

  flink_properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  # 50 fixed customer IDs so each customer accumulates history quickly.
  customer_options = join(",", [for i in range(1, 51) : format("''CUST-%04d''", i)])

  # Normal amounts split into four bands to create a realistic spend distribution,
  # then concatenated with different repetition counts so the per-customer per-15s-window
  # MAX(amount) series fed to ML_DETECT_ANOMALIES remains statistically stable:
  #
  #   small   (5 vals, 160×) → 800 draws  — coffee, snacks, quick lunch
  #   medium  (5 vals, 200×) → 1,000 draws — sit-down meal, gas, subscriptions
  #   large   (5 vals, 240×) → 1,200 draws — grocery run, online order
  #   ceiling (6 vals, 400×) → 2,400 draws — bigger purchases, ceiling of normal
  #
  # Total normal draws: 5,400; plus 8 spike values → spike rate ≈ 0.15%.
  # P(at least one ceiling value in a 15-draw window) ≈ 99.98%, so the windowed MAX
  # lands in the $375–$480 base range on nearly every window and ARIMA stays stable.
  #
  # These are BASE amounts. The transactions_insert step scales each by ×0.1/×1/×10
  # so the three tiers have heterogeneous baselines:
  #   SMALL_SPENDER  (~$1–$48 normal,  ~$125–$890 fraud)
  #   MAINSTREAM     (~$11–$480 normal, ~$1,250–$8,895 fraud)
  #   BIG_SPENDER    (~$105–$4,800 normal, ~$12,500–$88,950 fraud)
  normal_small   = ["10.50", "13.75", "18.00", "24.50", "32.00"]
  normal_medium  = ["45.00", "58.00", "72.50", "88.00", "105.00"]
  normal_large   = ["135.00", "168.00", "205.00", "255.00", "315.00"]
  normal_ceiling = ["375.00", "415.00", "440.00", "460.00", "475.00", "480.00"]
  spike_amounts  = ["1250.75", "2480.90", "3640.15", "4510.75", "5925.40", "6840.20", "7720.50", "8895.30"]
  amount_options = join(",", [for v in concat(
    flatten([for i in range(160) : local.normal_small]),
    flatten([for i in range(200) : local.normal_medium]),
    flatten([for i in range(240) : local.normal_large]),
    flatten([for i in range(400) : local.normal_ceiling]),
    local.spike_amounts
  ) : "''${v}''"])
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink DDL: transactions_gen (faker data generator)
#
# Generates synthetic card transactions with the Flink faker connector. The
# faker connector is virtual: every statement that reads it generates its own
# independent random stream, so we persist it once into the real `transactions`
# table and all walkthrough SQL reads from that one consistent Kafka-backed
# stream (same pattern as Lab 1's machine_sensor_raw -> cnc_machine_signals).
#
# - amount: realistic 4-band distribution ($10–$480 base) with ceiling-weighted
#   repetition plus ~0.15% varied spike amounts ($1,250–$8,895 base) — fraud signal
# - transaction_ts declared as event-time attribute via WATERMARK
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_transactions_gen" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab2-create-transactions-gen-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `transactions_gen` (
      `transaction_id`  VARCHAR(2147483647) NOT NULL,
      `customer_id`     VARCHAR(2147483647) NOT NULL,
      `merchant`        VARCHAR(2147483647) NOT NULL,
      `amount`          DOUBLE              NOT NULL,
      `transaction_ts`  TIMESTAMP(3)        NOT NULL,
      WATERMARK FOR `transaction_ts` AS `transaction_ts` - INTERVAL '5' SECOND
    ) WITH (
      'connector'       = 'faker',
      'rows-per-second' = '50',
      'fields.transaction_id.expression' = '#{Internet.uuid}',
      'fields.customer_id.expression'    = '#{Options.option ${local.customer_options}}',
      'fields.merchant.expression'       = '#{Company.name}',
      'fields.amount.expression'         = '#{Options.option ${local.amount_options}}',
      'fields.transaction_ts.expression' = '#{date.past ''5'',''SECONDS''}'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  lifecycle {
    prevent_destroy = false
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink DDL: transactions (real Kafka-backed table)
#
# The walkthrough SQL reads from this table so every downstream statement sees
# the same stream of events.
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_transactions" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab2-create-transactions-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `transactions` (
      `transaction_id`   VARCHAR(2147483647) NOT NULL,
      `customer_id`      VARCHAR(2147483647) NOT NULL,
      `customer_segment` VARCHAR(2147483647) NOT NULL,
      `merchant`         VARCHAR(2147483647) NOT NULL,
      `amount`           DOUBLE              NOT NULL,
      `transaction_ts`   TIMESTAMP(3)        NOT NULL,
      WATERMARK FOR `transaction_ts` AS `transaction_ts` - INTERVAL '5' SECOND
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_flink_statement.create_transactions_gen]
}

# Continuously persist the generated events into the real transactions table,
# scaling each customer's amount into a spending tier and tagging the segment.
#
# The faker generates a single global amount distribution; we multiply it by a
# per-customer factor derived from the numeric ID suffix so the 50 customers fall
# into three heterogeneous spending tiers:
#   - SMALL_SPENDER (CUST-0001..0017, ×0.1):  normal ~$1-$48,       fraud ~$125-$890
#   - MAINSTREAM    (CUST-0018..0034, ×1.0):  normal ~$11-$480,     fraud ~$1,250-$8,895
#   - BIG_SPENDER   (CUST-0035..0050, ×10):   normal ~$105-$4,800,  fraud ~$12,500-$88,950
#
# customer_segment is a human-readable label only — ML_DETECT_ANOMALIES never sees
# it; each model partitions by customer_id and learns that customer's normal from
# its own history. ROUND(...,2) keeps amounts exact so the fraud_transactions join
# matches on equality.
resource "confluent_flink_statement" "transactions_insert" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab2-transactions-insert-${local.random_id}"

  statement = <<-SQL
    INSERT INTO `transactions`
    SELECT
      `transaction_id`,
      `customer_id`,
      CASE
        WHEN CAST(SUBSTRING(`customer_id`, 6) AS INT) <= 17 THEN 'SMALL_SPENDER'
        WHEN CAST(SUBSTRING(`customer_id`, 6) AS INT) <= 34 THEN 'MAINSTREAM'
        ELSE 'BIG_SPENDER'
      END AS `customer_segment`,
      `merchant`,
      ROUND(`amount` * CASE
        WHEN CAST(SUBSTRING(`customer_id`, 6) AS INT) <= 17 THEN 0.1
        WHEN CAST(SUBSTRING(`customer_id`, 6) AS INT) <= 34 THEN 1.0
        ELSE 10.0
      END, 2) AS `amount`,
      `transaction_ts`
    FROM `transactions_gen`;
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.create_transactions_gen,
    confluent_flink_statement.create_transactions,
  ]
}
