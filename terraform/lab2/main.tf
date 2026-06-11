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

  # Normal amounts (10 values, $18-$480) repeated 600x, plus 8 varied "spike"
  # amounts ($1,250-$8,895, deliberately non-round) appearing once each => ~0.13%
  # of transactions are an anomalous spike. Varying the spike size means the demo
  # surfaces a realistic spread of fraudulent amounts rather than one fixed value.
  # Spikes are kept rare (~one per customer per ~13 min) so a spike doesn't linger
  # in the model's training window long enough to inflate its bounds and mask the
  # next spike. The smallest spike ($1,250) still sits well above the normal $480
  # ceiling, so a matured per-customer model flags it.
  normal_amounts = ["18.50", "42.00", "75.25", "120.00", "165.40", "210.75", "280.00", "340.90", "410.50", "480.00"]
  spike_amounts  = ["1250.75", "2480.90", "3640.15", "4510.75", "5925.40", "6840.20", "7720.50", "8895.30"]
  amount_options = join(",", [for v in concat(flatten([for i in range(600) : local.normal_amounts]), local.spike_amounts) : "''${v}''"])
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
# - amount: normal values ($18-$480) plus ~0.13% varied "spike" amounts
#   ($1,250-$8,895) — the fraud signal this lab detects
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
      `transaction_id`  VARCHAR(2147483647) NOT NULL,
      `customer_id`     VARCHAR(2147483647) NOT NULL,
      `merchant`        VARCHAR(2147483647) NOT NULL,
      `amount`          DOUBLE              NOT NULL,
      `transaction_ts`  TIMESTAMP(3)        NOT NULL,
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

# Continuously persist the generated events into the real transactions table.
resource "confluent_flink_statement" "transactions_insert" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab2-transactions-insert-${local.random_id}"

  statement = <<-SQL
    INSERT INTO `transactions` SELECT * FROM `transactions_gen`;
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
