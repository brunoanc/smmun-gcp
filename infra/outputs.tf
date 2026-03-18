output "api_service_account_email" {
  value = google_service_account.api.email
}

output "worker_service_account_email" {
  value = google_service_account.worker.email
}

output "api_url" {
  value = google_cloud_run_v2_service.api.uri
}

output "pubsub_topic_name" {
  value = google_pubsub_topic.inscripciones.name
}

output "api_domain_dns_records" {
  value = google_cloud_run_domain_mapping.api.status[0].resource_records
}
