variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bucket_name" {
  type    = string
  default = "smmun-inscripciones-2026"
}

variable "firestore_database_name" {
  type    = string
  default = "(default)"
}

variable "firestore_collection_name" {
  type    = string
  default = "inscripciones"
}

variable "pubsub_topic_name" {
  type    = string
  default = "smmun-inscripciones-topic"
}

variable "resend_secret_name" {
  type    = string
  default = "resend-api-key"
}

variable "api_service_name" {
  type    = string
  default = "smmun-api"
}

variable "api_domain" {
  type    = string
}

variable "worker_function_name" {
  type    = string
  default = "smmun-worker-function"
}
