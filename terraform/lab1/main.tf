data "terraform_remote_state" "core" {
  backend = "local"
  config  = { path = "../core/terraform.tfstate" }
}

locals {
  cloud_provider = data.terraform_remote_state.core.outputs.cloud_provider
  cloud_region   = data.terraform_remote_state.core.outputs.cloud_region
}

data "confluent_organization" "main" {}

# Get Flink region data
data "confluent_flink_region" "lab3_flink_region" {
  cloud  = upper(local.cloud_provider)
  region = local.cloud_region
}
# Add lab-specific resources below


# Create machine_sensor_raw table with WATERMARK
resource "confluent_flink_statement" "machine_sensor_raw_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab3_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "machine-sensor-raw-create-table"

  statement = <<-EOT
    CREATE TABLE machine_sensor_raw (
  machine_id STRING,
  motor_current DOUBLE,
  rpm INT,
  voltage INT,
  vibration_raw DOUBLE,
  -- Use a standard timestamp field that the Faker connector can populate
  ts TIMESTAMP_LTZ(3),
  -- Define the watermark on the timestamp field for time-series ML functions
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'faker',
  'fields.machine_id.expression' = '#{regexify ''CNC-(101|102|103)''}',
  'fields.motor_current.expression' = '#{number.random_double ''2'',''10'',''15''}',
  'fields.rpm.expression' = '#{number.number_between ''1400'',''1500''}',
  'fields.voltage.expression' = '220',
  'fields.vibration_raw.expression' = '#{number.random_double ''4'',''0'',''1''}',
  'fields.ts.expression' = '#{date.past ''10'',''SECONDS''}'
);
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

# Create machine_sensor_raw table with WATERMARK
resource "confluent_flink_statement" "cnc_machine_signals_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab3_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "cnc-machine-signals-create-table"

  statement = <<-EOT
  CREATE TABLE cnc_machine_signals (
  machine_id     STRING,
  ts             TIMESTAMP_LTZ(3),
  rpm            DOUBLE,
  vibration_raw  DOUBLE,
  motor_current  DOUBLE,
  voltage        INT,
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
);
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core,
    confluent_flink_statement.machine_sensor_raw_table
  ]
}

resource "confluent_flink_statement" "cnc_machine_signals_insert" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab3_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "cnc-machine-signals-insert"

  statement = <<-EOT
 INSERT INTO cnc_machine_signals
SELECT 
    machine_id,
    ts,
    1450 + (5 * SIN(CAST(EXTRACT(SECOND FROM ts) AS DOUBLE))) AS rpm,
    CASE 
        WHEN EXTRACT(SECOND FROM ts) BETWEEN 50 AND 55
        THEN 0.85 + (RAND() * 0.1)
        ELSE 0.02 + (RAND() * 0.01)
    END AS vibration_raw,
    motor_current,
    voltage
FROM machine_sensor_raw;
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core,
    confluent_flink_statement.cnc_machine_signals_table,
    confluent_flink_statement.machine_sensor_raw_table
  ]
}
