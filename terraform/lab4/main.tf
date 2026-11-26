data "terraform_remote_state" "core" {
  backend = "local"
  config  = { path = "../core/terraform.tfstate" }
}

locals {
  core = data.terraform_remote_state.core.outputs

  flink_properties = {
    "sql.current-catalog"  = local.core.confluent_environment_display_name
    "sql.current-database" = local.core.confluent_kafka_cluster_display_name
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Source table: etth1_readings
#
# One row per minute of transformer sensor readings (ETTh1 benchmark, upsampled
# from hourly to minutely). `uv run deploy lab4` loads the first ~3 days right
# after this table is created; `uv run lab4-dashboard` streams the rest live.
#
# - Single bucket: one transformer = one ordered time series.
# - Event time comes from `reading_time` (the sensor timestamp), not arrival time.
# - simulated_fault marks readings changed by the dashboard's "Simulate fault"
#   buttons, so injected readings can never be mistaken for real ones.
# - Infinite retention ('0'): the readings are from 2017, so time-based retention
#   would otherwise delete them as soon as they land.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "etth1_readings_table" {
  organization { id = local.core.confluent_organization_id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }

  statement_name = "lab4-etth1-readings-table-${local.core.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `etth1_readings` (
      `transformer_id` STRING NOT NULL,
      `reading_time`   TIMESTAMP_LTZ(3) NOT NULL,
      `HUFL`           DOUBLE NOT NULL,
      `HULL`           DOUBLE NOT NULL,
      `MUFL`           DOUBLE NOT NULL,
      `MULL`           DOUBLE NOT NULL,
      `LUFL`           DOUBLE NOT NULL,
      `LULL`           DOUBLE NOT NULL,
      `OT`             DOUBLE NOT NULL,
      `simulated_fault` BOOLEAN NOT NULL,
      WATERMARK FOR `reading_time` AS `reading_time` - INTERVAL '1' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'value.format'         = 'avro-registry',
      'kafka.retention.time' = '0',
      'scan.startup.mode'    = 'earliest-offset'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.core.confluent_flink_rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }
}

# The AI_FORECAST / AI_DETECT_ANOMALIES pipelines are intentionally not here:
# you run them yourself in the Flink SQL workspace (Lab4-Walkthrough.md, steps
# 2 and 3), or with `uv run lab4-flink`.
