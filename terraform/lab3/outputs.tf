output "confluent_environment_id" {
  value = data.terraform_remote_state.core.outputs.confluent_environment_id
}
output "confluent_kafka_cluster_id" {
  value = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
}
output "confluent_flink_compute_pool_id" {
  value = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
}
