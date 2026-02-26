variable "cloud_provider" {
  description = "Cloud provider for deployment (aws or azure)"
  type        = string

  validation {
    condition     = contains(["aws", "azure"], var.cloud_provider)
    error_message = "cloud_provider must be 'aws' or 'azure'."
  }
}

variable "cloud_region" {
  description = "Cloud region for deployment (must support MongoDB Atlas M0 free tier)"
  type        = string
  default     = "us-east-1"
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}
