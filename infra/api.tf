# Cloud Run API resource
resource "google_cloud_run_v2_service" "api" {
  name     = var.api_service_name
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.api.email

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello"

      env {
        name  = "FIRESTORE_COLLECTION_NAME"
        value = var.firestore_collection_name
      }

      env {
        name  = "COMPROBANTES_BUCKET_NAME"
        value = google_storage_bucket.comprobantes.name
      }

      env {
        name  = "PUB_SUB_TOPIC_NAME"
        value = google_pubsub_topic.inscripciones.name
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image
    ]
  }

  depends_on = [
    google_project_service.services,
    google_project_iam_member.api_storage_creator,
    google_project_iam_member.api_datastore_user,
    google_project_iam_member.api_pubsub_publisher
  ]
}

# Allow public access
resource "google_cloud_run_v2_service_iam_member" "api_public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Create domain mapping
resource "google_cloud_run_domain_mapping" "api" {
  location = var.region
  name     = var.api_domain

  metadata {
    namespace = var.project_id
  }

  spec {
    route_name = google_cloud_run_v2_service.api.name
  }
}
