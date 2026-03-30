# Functions source bucket
resource "google_storage_bucket" "functions_source" {
  name                        = "${var.project_id}-functions-src"
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  depends_on = [google_project_service.services]
}

# Upload code to bucket
resource "google_storage_bucket_object" "worker_zip" {
  name   = "worker-${filemd5("${path.module}/worker.zip")}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = "${path.module}/worker.zip"
}

resource "google_storage_bucket_object" "outbox_zip" {
  name   = "outbox-${filemd5("${path.module}/outbox.zip")}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = "${path.module}/outbox.zip"
}

# Cloud Run functions worker resource
resource "google_cloudfunctions2_function" "worker" {
  name     = var.worker_function_name
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "pubsub_handler"

    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.worker_zip.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.worker.email
    available_memory      = "512M"
    timeout_seconds       = 60

    environment_variables = {
      FIRESTORE_COLLECTION_NAME     = var.firestore_collection_name
      COMPROBANTES_BUCKET_NAME      = google_storage_bucket.comprobantes.name
      DELEGACIONES_SPREADSHEET_ID   = var.delegaciones_spreadsheet_id
      DELEGACIONES_SHEET_ID         = var.delegaciones_sheet_id
      DELEGACIONES_TABLE_ID         = var.delegaciones_table_id
      FACULTY_SPREADSHEET_ID        = var.faculty_spreadsheet_id
      FACULTY_GENERAL_SHEET_ID      = var.faculty_general_sheet_id
      FACULTY_GENERAL_TABLE_ID      = var.faculty_general_table_id
      FACULTY_DELEGACIONES_SHEET_ID = var.faculty_delegaciones_sheet_id
      FACULTY_DELEGACIONES_TABLE_ID = var.faculty_delegaciones_table_id
    }

    secret_environment_variables {
      key        = "RESEND_API_KEY"
      project_id = var.project_id
      secret     = google_secret_manager_secret.resend_api_key.secret_id
      version    = "latest"
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.inscripciones.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }

  depends_on = [
    google_project_service.services,
    google_firestore_database.default,
    google_project_iam_member.worker_storage_viewer,
    google_project_iam_member.worker_datastore_user,
    google_pubsub_topic_iam_member.worker_pubsub_publisher,
    google_project_iam_member.worker_token_creator,
    google_secret_manager_secret_iam_member.worker_secret_access
  ]
}

resource "google_cloudfunctions2_function" "publisher" {
  name     = var.publisher_function_name
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "outbox_created_handler"

    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.outbox_zip.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.worker.email
    available_memory      = "256M"
    timeout_seconds       = 60

    environment_variables = {
      OUTBOX_COLLECTION_NAME = var.outbox_collection_name
      PUB_SUB_TOPIC_NAME     = google_pubsub_topic.inscripciones.name
    }
  }

  event_trigger {
    service_account_email = google_service_account.worker.email
    trigger_region        = var.region
    event_type            = "google.cloud.firestore.document.v1.created"
    retry_policy          = "RETRY_POLICY_RETRY"

    event_filters {
      attribute = "database"
      value     = google_firestore_database.default.name
    }

    event_filters {
      attribute = "namespace"
      value     = "(default)"
    }

    event_filters {
      attribute = "document"
      value     = "${var.outbox_collection_name}/{outboxId}"
      operator  = "match-path-pattern"
    }
  }

  depends_on = [
    google_project_service.services,
    google_firestore_database.default,
    google_project_iam_member.firestore_eventarc_publisher,
    google_project_iam_member.worker_datastore_user,
    google_pubsub_topic_iam_member.worker_pubsub_publisher,
    google_project_iam_member.worker_eventarc_receiver
  ]
}

resource "google_cloudfunctions2_function" "outbox_sweeper" {
  name     = var.outbox_sweeper_function_name
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "outbox_sweep_handler"

    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.outbox_zip.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.worker.email
    available_memory      = "256M"
    timeout_seconds       = 60

    environment_variables = {
      OUTBOX_COLLECTION_NAME = var.outbox_collection_name
      PUB_SUB_TOPIC_NAME     = google_pubsub_topic.inscripciones.name
    }
  }

  event_trigger {
    service_account_email = google_service_account.worker.email
    trigger_region        = var.region
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = google_pubsub_topic.outbox_sweep.id
    retry_policy          = "RETRY_POLICY_RETRY"
  }

  depends_on = [
    google_project_service.services,
    google_firestore_database.default,
    google_project_iam_member.worker_datastore_user,
    google_pubsub_topic_iam_member.worker_pubsub_publisher,
    google_project_iam_member.worker_eventarc_receiver
  ]
}

resource "google_cloud_scheduler_job" "outbox_sweep" {
  name        = "smmun-outbox-sweep"
  description = "Periodically retries pending or stale outbox rows."
  region      = var.region
  schedule    = var.outbox_sweep_schedule
  time_zone   = "Etc/UTC"

  pubsub_target {
    topic_name = google_pubsub_topic.outbox_sweep.id
    data       = base64encode(jsonencode({ trigger = "outbox_sweep" }))
  }

  depends_on = [
    google_project_service.services,
    google_cloudfunctions2_function.outbox_sweeper,
    google_pubsub_topic.outbox_sweep,
    google_pubsub_topic_iam_member.scheduler_outbox_sweep_publisher
  ]
}
