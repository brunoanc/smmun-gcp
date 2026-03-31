"""GCP/runtime helpers for the worker: clients, logging, claiming, and dispatch."""

import json
import logging
import os
import traceback
from google.auth import default
from google.auth.transport.requests import Request as TransportRequest
from google.cloud import firestore, storage
from googleapiclient.discovery import build
import resend

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
logger = logging.getLogger(__name__)
LOG_COMPONENT = "worker"


# Format structured logs in the JSON shape expected by GCP
def _to_json_log(event: str, severity: str, **fields) -> str:
    request_id = fields.get("request_id")
    payload = {
        "severity": severity,
        "event": event,
        "component": LOG_COMPONENT,
        **fields,
    }

    if request_id:
        payload["logging.googleapis.com/trace"] = f"projects/{PROJECT_ID}/traces/{request_id}"

    return json.dumps(payload, ensure_ascii=False, default=str)


# Log an informational event
def log_info(event: str, **fields):
    logger.info(_to_json_log(event, "INFO", **fields))


# Log a warning event
def log_warning(event: str, **fields):
    logger.warning(_to_json_log(event, "WARNING", **fields))


# Log an exception with stack trace context
def log_exception(event: str, **fields):
    logger.error(_to_json_log(event, "ERROR", error=traceback.format_exc(), **fields))


# GCP clients
# Sheets API
google_credentials, PROJECT_ID = default()
sheets_service = build("sheets", "v4", credentials=google_credentials)

# Resend API
resend.api_key = os.environ["RESEND_API_KEY"]

# Firestore DB
FIRESTORE_COLLECTION_NAME = os.environ["FIRESTORE_COLLECTION_NAME"]
db_client = firestore.Client()
db_collection = db_client.collection(FIRESTORE_COLLECTION_NAME)

# Cloud Storage
COMPROBANTES_BUCKET_NAME = os.environ["COMPROBANTES_BUCKET_NAME"]
storage_client = storage.Client()
comprobantes_bucket = storage_client.bucket(COMPROBANTES_BUCKET_NAME)


# Claim a submission for worker processing
def claim_submission(doc_ref):
    transaction = db_client.transaction()

    # Move a submission to processing if it has not already reached a terminal or active state
    @firestore.transactional
    def _claim(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            return False

        data = doc.to_dict()

        # Failed submissions are also skipped because downstream side effects are not idempotent.
        if data.get("status") in ["processing", "completed", "failed"]:
            return False

        transaction.update(doc_ref, {"status": "processing"})
        return True

    return _claim(transaction, doc_ref)


# Process a Pub/Sub delivery for a submission
def process_submission(submission_id: str, request_id: str, submission_type: str | None = None):
    from business import process_delegacion_submission, process_faculty_submission

    log_info(
        "submission_processing_started",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
    )

    google_credentials.refresh(TransportRequest())

    doc_ref = db_collection.document(submission_id)

    try:
        if not claim_submission(doc_ref):
            log_info(
                "submission_skipped",
                request_id=request_id,
                submission_id=submission_id,
                submission_type=submission_type,
                reason="already_processing_or_completed",
            )
            return

        data = doc_ref.get().to_dict()
        submission_type = data.get("type")
        log_info(
            "submission_type_detected",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )

        if submission_type == "delegacion":
            process_delegacion_submission(data, request_id)
        else:
            process_faculty_submission(data, request_id)

        doc_ref.update({"status": "completed"})
        log_info(
            "submission_processing_finished",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )
        log_info(
            "submission_terminal_success",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            final_status="completed",
        )

    except Exception as exc:
        log_exception(
            "submission_processing_failed",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )
        doc_ref.update({"status": "failed", "error_message": str(exc)})
