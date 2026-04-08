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
  schema_registry_id          = data.terraform_remote_state.core.outputs.confluent_schema_registry_id
  schema_registry_rest        = data.terraform_remote_state.core.outputs.confluent_schema_registry_rest_endpoint
  schema_registry_api_key     = data.terraform_remote_state.core.outputs.app_manager_schema_registry_api_key
  schema_registry_api_secret  = data.terraform_remote_state.core.outputs.app_manager_schema_registry_api_secret
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
# Flink DDL: lab3_tower_traffic (source table — AVRO from Python datagen)
#
# All columns are NOT NULL → Flink generates non-nullable Avro types.
# No WITH clause → Flink defaults to avro-registry and auto-registers the schema.
# Uses $rowtime (Kafka message timestamp) for event-time processing.
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
      `region`               STRING NOT NULL,
      `ts_ms`                BIGINT NOT NULL,
      `throughput_mbps`      DOUBLE NOT NULL,
      `active_users`         INT NOT NULL,
      `signal_strength_dbm`  INT NOT NULL,
      `capacity_mbps`        DOUBLE NOT NULL
    )
    DISTRIBUTED INTO 6 BUCKETS WITH ('value.format' = 'avro-registry');
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
# NOTE: Flink CTAS pipelines (lab3_tower_agg, lab3_forecasts, lab3_capacity_alerts)
# are deployed separately via `uv run lab3-flink` AFTER the datagen has produced data.
# This avoids "Column not found" errors caused by Flink's data dependency for
# schema resolution.
