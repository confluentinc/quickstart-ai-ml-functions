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
  kafka_cluster_id    = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
  kafka_api_key       = data.terraform_remote_state.core.outputs.app_manager_kafka_api_key
  kafka_api_secret    = data.terraform_remote_state.core.outputs.app_manager_kafka_api_secret
  kafka_rest_endpoint = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_rest_endpoint
  random_id           = data.terraform_remote_state.core.outputs.random_id

  flink_properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Kafka Topics — 6 partitions each, keyed by tower_id
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_kafka_topic" "lab3_tower_traffic" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  topic_name       = "lab3_tower_traffic"
  partitions_count = 6
  rest_endpoint    = local.kafka_rest_endpoint
  credentials {
    key    = local.kafka_api_key
    secret = local.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "lab3_tower_agg" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  topic_name       = "lab3_tower_agg"
  partitions_count = 6
  rest_endpoint    = local.kafka_rest_endpoint
  credentials {
    key    = local.kafka_api_key
    secret = local.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "lab3_forecasts" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  topic_name       = "lab3_forecasts"
  partitions_count = 6
  rest_endpoint    = local.kafka_rest_endpoint
  credentials {
    key    = local.kafka_api_key
    secret = local.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "lab3_capacity_alerts" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  topic_name       = "lab3_capacity_alerts"
  partitions_count = 6
  rest_endpoint    = local.kafka_rest_endpoint
  credentials {
    key    = local.kafka_api_key
    secret = local.kafka_api_secret
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink DDL: lab3_tower_traffic (source table — JSON from Python datagen)
#
# The Python generator publishes ts_ms as epoch-milliseconds to avoid any
# ISO-8601 string parsing ambiguity. A computed column converts it to the
# TIMESTAMP_LTZ(3) type used by the windowing and ML functions.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "lab3_tower_traffic_table" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab3-tower-traffic-table-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `lab3_tower_traffic` (
      `tower_id`             STRING NOT NULL,
      `region`               STRING,
      `ts_ms`                BIGINT,
      `throughput_mbps`      DOUBLE,
      `active_users`         INT,
      `signal_strength_dbm`  INT,
      `capacity_mbps`        DOUBLE,
      `ts`                   AS TO_TIMESTAMP_LTZ(`ts_ms`, 3),
      WATERMARK FOR `ts` AS `ts` - INTERVAL '10' SECOND
    ) WITH (
      'kafka.partitions' = '6',
      'value.format'     = 'json'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [confluent_kafka_topic.lab3_tower_traffic]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink: 5-minute tumble-window aggregation per tower → lab3_tower_agg
#
# Produces one row per (tower_id, 5-min window) with avg/max throughput and
# the constant tower capacity — these become the time series fed to ML_FORECAST.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "lab3_tower_agg_pipeline" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab3-tower-agg-pipeline-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE `lab3_tower_agg`
    WITH ('kafka.partitions' = '6')
    AS
    SELECT
      tower_id,
      region,
      window_start,
      window_end,
      MAX(capacity_mbps)                            AS capacity_mbps,
      AVG(throughput_mbps)                          AS avg_throughput_mbps,
      MAX(throughput_mbps)                          AS max_throughput_mbps,
      CAST(AVG(CAST(active_users AS DOUBLE)) AS DOUBLE) AS avg_active_users
    FROM TABLE(
      TUMBLE(TABLE `lab3_tower_traffic`, DESCRIPTOR(`ts`), INTERVAL '5' MINUTES)
    )
    GROUP BY tower_id, region, window_start, window_end;
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_kafka_topic.lab3_tower_agg,
    confluent_flink_statement.lab3_tower_traffic_table,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink: ML_FORECAST per tower → lab3_forecasts
#
# PARTITION BY tower_id gives each of the 10 towers its own independent
# ARIMA model — the key differentiator that makes per-tower forecasting
# practical without any offline model training.
#
# horizon=6 means the model predicts 6 × 5-min windows = 30 minutes ahead,
# giving ops teams actionable lead time before congestion hits customers.
# minTrainingSize=10 lets each tower's model start forecasting after only
# 10 aggregated windows (~50 minutes of data).
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "lab3_forecasts_pipeline" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab3-forecasts-pipeline-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE `lab3_forecasts`
    WITH ('kafka.partitions' = '6')
    AS
    WITH forecasted AS (
      SELECT
        tower_id,
        region,
        window_end,
        capacity_mbps,
        avg_throughput_mbps,
        ML_FORECAST(
          avg_throughput_mbps,
          window_end,
          JSON_OBJECT(
            'horizon'         VALUE 6,
            'minTrainingSize' VALUE 10
          )
        ) OVER (
          PARTITION BY tower_id
          ORDER BY window_end
          ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS fc
      FROM `lab3_tower_agg`
    )
    SELECT
      tower_id,
      region,
      window_end                 AS ts,
      capacity_mbps,
      avg_throughput_mbps,
      fc.forecast                AS forecast_throughput_mbps,
      fc.confidenceLower         AS confidence_lower_mbps,
      fc.confidenceUpper         AS confidence_upper_mbps
    FROM forecasted;
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_kafka_topic.lab3_forecasts,
    confluent_flink_statement.lab3_tower_agg_pipeline,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink: Capacity alert → lab3_capacity_alerts
#
# Emits an alert row whenever the forecasted throughput exceeds 85 % of a
# tower's capacity, giving operations teams ~30 minutes of lead time
# (equal to ML_FORECAST horizon × 5-min window size).
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "lab3_capacity_alerts_pipeline" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "lab3-capacity-alerts-pipeline-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE `lab3_capacity_alerts`
    WITH ('kafka.partitions' = '6')
    AS
    SELECT
      tower_id,
      region,
      ts,
      capacity_mbps,
      avg_throughput_mbps,
      forecast_throughput_mbps,
      ROUND((forecast_throughput_mbps / capacity_mbps) * 100.0, 1) AS forecast_utilization_pct,
      'CAPACITY_WARNING'                                            AS alert_type,
      CONCAT(
        'Tower ', tower_id,
        ' forecasted at ',
        CAST(ROUND((forecast_throughput_mbps / capacity_mbps) * 100.0, 1) AS STRING),
        '% capacity — congestion risk in ~30 min'
      )                                                             AS alert_message
    FROM `lab3_forecasts`
    WHERE forecast_throughput_mbps > (capacity_mbps * 0.85);
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_kafka_topic.lab3_capacity_alerts,
    confluent_flink_statement.lab3_forecasts_pipeline,
  ]
}
