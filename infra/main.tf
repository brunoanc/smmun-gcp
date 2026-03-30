# Enable APIs
resource "google_project_service" "services" {
  for_each = toset([
    "run.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "pubsub.googleapis.com",
    "firestore.googleapis.com",
    "secretmanager.googleapis.com",
    "sheets.googleapis.com",
    "storage.googleapis.com",
    "eventarc.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# Bucket for storing comprobantes
resource "google_storage_bucket" "comprobantes" {
  name                        = var.bucket_name
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age = 30
    }

    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  depends_on = [google_project_service.services]
}

# Firestore database
resource "google_firestore_database" "default" {
  project     = var.project_id
  name        = var.firestore_database_name
  location_id = var.region
  type        = "FIRESTORE_NATIVE"

  depends_on = [google_project_service.services]
}

# Resend API key secret manager
resource "google_secret_manager_secret" "resend_api_key" {
  secret_id = var.resend_secret_name

  replication {
    auto {}
  }

  depends_on = [google_project_service.services]
}

# Pub/sub topic to trigger worker
resource "google_pubsub_topic" "inscripciones" {
  name       = var.pubsub_topic_name
  depends_on = [google_project_service.services]
}

resource "google_pubsub_topic" "outbox_sweep" {
  name       = var.outbox_sweep_topic_name
  depends_on = [google_project_service.services]
}

resource "google_firestore_index" "outbox_status_updated_at" {
  project    = var.project_id
  database   = google_firestore_database.default.name
  collection = var.outbox_collection_name

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "updated_at"
    order      = "ASCENDING"
  }

  depends_on = [google_project_service.services, google_firestore_database.default]
}

resource "google_firestore_index" "outbox_status_publish_lease_expires_at" {
  project    = var.project_id
  database   = google_firestore_database.default.name
  collection = var.outbox_collection_name

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "publish_lease_expires_at"
    order      = "ASCENDING"
  }

  depends_on = [google_project_service.services, google_firestore_database.default]
}

# Artifact registry
resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "smmun"
  description   = "SMMUN container images"
  format        = "DOCKER"

  depends_on = [google_project_service.services]
}
