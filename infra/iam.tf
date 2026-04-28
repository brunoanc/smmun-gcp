data "google_project" "current" {
  project_id = var.project_id
}

# Service account for the Cloud Run API
resource "google_service_account" "api" {
  account_id   = "smmun-api-sa"
  display_name = "SMMUN API Service Account"

  depends_on = [google_project_service.services]
}

# Service account for the Cloud Run functions worker
resource "google_service_account" "worker" {
  account_id   = "smmun-worker-sa"
  display_name = "SMMUN Worker Service Account"

  depends_on = [google_project_service.services]
}

# Roles for API service account
resource "google_storage_bucket_iam_member" "api_comprobantes_object_user" {
  bucket = google_storage_bucket.comprobantes.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.api.email}"

  depends_on = [google_project_service.services]
}

resource "google_project_iam_member" "api_datastore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.api.email}"

  depends_on = [google_project_service.services]
}

# Roles for worker service account
resource "google_storage_bucket_iam_member" "worker_comprobantes_object_viewer" {
  bucket = google_storage_bucket.comprobantes.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker.email}"

  depends_on = [google_project_service.services]
}

resource "google_project_iam_member" "worker_datastore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.worker.email}"

  depends_on = [google_project_service.services]
}

resource "google_service_account_iam_member" "worker_signs_for_itself" {
  service_account_id = google_service_account.worker.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.worker.email}"

  depends_on = [google_project_service.services]
}

resource "google_pubsub_topic_iam_member" "worker_pubsub_publisher" {
  topic  = google_pubsub_topic.inscripciones.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.worker.email}"

  depends_on = [google_project_service.services]
}

resource "google_project_iam_member" "worker_eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.worker.email}"

  depends_on = [google_project_service.services]
}

resource "google_cloud_run_v2_service_iam_member" "publisher_trigger_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.publisher.service_config[0].service
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_cloud_run_v2_service_iam_member" "outbox_sweeper_trigger_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.outbox_sweeper.service_config[0].service
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_pubsub_topic_iam_member" "scheduler_outbox_sweep_publisher" {
  topic  = google_pubsub_topic.outbox_sweep.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"

  depends_on = [google_project_service.services]
}

resource "google_project_iam_member" "firestore_eventarc_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-firestore.iam.gserviceaccount.com"

  depends_on = [google_project_service.services]
}

resource "google_secret_manager_secret_iam_member" "worker_secret_access" {
  secret_id = google_secret_manager_secret.resend_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker.email}"

  depends_on = [google_project_service.services]
}
