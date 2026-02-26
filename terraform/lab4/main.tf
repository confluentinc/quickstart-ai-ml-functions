data "terraform_remote_state" "core" {
  backend = "local"
  config = { path = "../core/terraform.tfstate" }
}

locals {
  cloud_provider = data.terraform_remote_state.core.outputs.cloud_provider
  cloud_region   = data.terraform_remote_state.core.outputs.cloud_region
}

# Add lab-specific resources below
