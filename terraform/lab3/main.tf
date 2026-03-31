data "terraform_remote_state" "core" {
  backend = "local"
  config = { path = "../core/terraform.tfstate" }
}

locals {
  flink_rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
}

# Add lab-specific resources below
