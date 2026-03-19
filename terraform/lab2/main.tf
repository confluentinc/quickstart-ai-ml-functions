data "terraform_remote_state" "core" {
  backend = "local"
  config  = { path = "../core/terraform.tfstate" }
}

locals {
  environment_id     = data.terraform_remote_state.core.outputs.confluent_environment_id
  compute_pool_id    = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  organization_id    = data.terraform_remote_state.core.outputs.confluent_organization_id
  service_account_id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  flink_rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  flink_api_key      = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
  flink_api_secret   = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  random_id          = data.terraform_remote_state.core.outputs.random_id

  flink_properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink DDL: payments (faker data generator)
#
# Streams synthetic payment events using the Flink faker connector.
# - 50 fixed customer IDs so each customer accumulates history quickly
# - amount: 191 normal values ($12-$110) + 1 anomalous spike ($8,750) ~0.5%
# - transaction_ts declared as event-time attribute via WATERMARK
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_payments" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab2-create-payments-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `payments` (
      `payment_id`        VARCHAR(2147483647) NOT NULL,
      `customer_id`       VARCHAR(2147483647) NOT NULL,
      `merchant_name`     VARCHAR(2147483647) NOT NULL,
      `merchant_category` VARCHAR(2147483647) NOT NULL,
      `amount`            DOUBLE              NOT NULL,
      `payment_method`    VARCHAR(2147483647) NOT NULL,
      `card_type`         VARCHAR(2147483647) NOT NULL,
      `channel`           VARCHAR(2147483647) NOT NULL,
      `transaction_type`  VARCHAR(2147483647) NOT NULL,
      `country_code`      VARCHAR(2147483647) NOT NULL,
      `transaction_ts`    TIMESTAMP(3)        NOT NULL,
      WATERMARK FOR `transaction_ts` AS `transaction_ts` - INTERVAL '5' SECOND
    ) WITH (
      'connector'       = 'faker',
      'rows-per-second' = '10',
      'fields.payment_id.expression'        = '#{Internet.uuid}',
      'fields.customer_id.expression'       = '#{Options.option ''CUST-0001'',''CUST-0002'',''CUST-0003'',''CUST-0004'',''CUST-0005'',''CUST-0006'',''CUST-0007'',''CUST-0008'',''CUST-0009'',''CUST-0010'',''CUST-0011'',''CUST-0012'',''CUST-0013'',''CUST-0014'',''CUST-0015'',''CUST-0016'',''CUST-0017'',''CUST-0018'',''CUST-0019'',''CUST-0020'',''CUST-0021'',''CUST-0022'',''CUST-0023'',''CUST-0024'',''CUST-0025'',''CUST-0026'',''CUST-0027'',''CUST-0028'',''CUST-0029'',''CUST-0030'',''CUST-0031'',''CUST-0032'',''CUST-0033'',''CUST-0034'',''CUST-0035'',''CUST-0036'',''CUST-0037'',''CUST-0038'',''CUST-0039'',''CUST-0040'',''CUST-0041'',''CUST-0042'',''CUST-0043'',''CUST-0044'',''CUST-0045'',''CUST-0046'',''CUST-0047'',''CUST-0048'',''CUST-0049'',''CUST-0050''}',
      'fields.merchant_name.expression'     = '#{Company.name}',
      'fields.merchant_category.expression' = '#{Options.option ''GROCERY'',''GROCERY'',''GROCERY'',''RESTAURANT'',''RESTAURANT'',''ELECTRONICS'',''TRAVEL'',''OTHER''}',
      'fields.amount.expression'            = '#{Options.option ''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''8750.00''}',
      'fields.payment_method.expression'    = '#{Options.option ''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''DEBIT_CARD'',''DEBIT_CARD'',''DEBIT_CARD'',''WIRE_TRANSFER''}',
      'fields.card_type.expression'         = '#{Options.option ''VISA'',''VISA'',''VISA'',''MASTERCARD'',''MASTERCARD'',''AMEX'',''DISCOVER''}',
      'fields.channel.expression'           = '#{Options.option ''IN_STORE'',''IN_STORE'',''IN_STORE'',''IN_STORE'',''ONLINE'',''ONLINE'',''MOBILE_APP'',''ATM''}',
      'fields.transaction_type.expression'  = '#{Options.option ''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''REFUND'',''CASH_ADVANCE''}',
      'fields.country_code.expression'      = '#{Options.option ''US'',''US'',''US'',''GB''}',
      'fields.transaction_ts.expression'    = '#{date.past ''5'',''SECONDS''}'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}
