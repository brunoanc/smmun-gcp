"""GCP/runtime helpers for the worker: clients, logging, claiming, and dispatch."""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
import traceback
from uuid import uuid4
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

# Worker checkpointing
WORKER_LEASE_SECONDS = int(os.getenv("WORKER_LEASE_SECONDS", "90"))
CHECKPOINT_STATUS_PENDING = "pending"
CHECKPOINT_STATUS_STARTED = "started"
CHECKPOINT_STATUS_COMPLETED = "completed"
CHECKPOINT_STATUS_FAILED = "failed"
REQUIRED_CHECKPOINTS_BY_TYPE = {
    "delegacion": ["sheets", "resend_confirmation"],
    "faculty": ["sheets", "resend_confirmation", "resend_internal_notification"],
}


class WorkerLeaseLost(RuntimeError):
    """Raised when a stale worker invocation no longer owns a submission."""


class WorkerLeaseActive(RuntimeError):
    """Raised when another active worker still owns a submission."""


def get_required_checkpoints(submission_type: str | None) -> list[str]:
    return REQUIRED_CHECKPOINTS_BY_TYPE.get(submission_type or "", REQUIRED_CHECKPOINTS_BY_TYPE["faculty"])


def get_default_checkpoints(submission_type: str | None) -> dict:
    return {
        checkpoint_name: {
            "status": CHECKPOINT_STATUS_PENDING,
            "started_at": None,
            "completed_at": None,
            "error_message": None,
            "updated_at": None,
        }
        for checkpoint_name in get_required_checkpoints(submission_type)
    }


def merge_default_checkpoints(data: dict) -> dict:
    checkpoints = data.get("worker_checkpoints") or {}
    merged = get_default_checkpoints(data.get("type"))

    for checkpoint_name, checkpoint_data in checkpoints.items():
        if isinstance(checkpoint_data, dict):
            merged[checkpoint_name] = {**merged.get(checkpoint_name, {}), **checkpoint_data}
        else:
            merged[checkpoint_name] = checkpoint_data

    return merged


# Claim a submission for worker processing
def claim_submission(doc_ref):
    transaction = db_client.transaction()

    # Move a submission to processing if it is new or if an older worker lease is stale.
    @firestore.transactional
    def _claim(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            return {"claimed": False, "reason": "missing"}

        data = doc.to_dict()
        status = data.get("status")

        # Failed submissions are also skipped because downstream side effects are not idempotent.
        if status in ["completed", "failed"]:
            return {"claimed": False, "reason": status}

        now = datetime.now(timezone.utc)

        if status == "processing":
            lease_expires_at = data.get("worker_lease_expires_at")

            if lease_expires_at is not None and lease_expires_at.astimezone(timezone.utc) > now:
                return {"claimed": False, "reason": "processing"}

        lease_owner = str(uuid4())
        checkpoints = merge_default_checkpoints(data)

        transaction.update(
            doc_ref,
            {
                "status": "processing",
                "worker_lease_owner": lease_owner,
                "worker_lease_expires_at": now + timedelta(seconds=WORKER_LEASE_SECONDS),
                "worker_started_at": now,
                "worker_attempt_count": firestore.Increment(1),
                "worker_checkpoints": checkpoints,
                "updated_at": now,
            },
        )
        data["worker_lease_owner"] = lease_owner
        data["worker_checkpoints"] = checkpoints
        return {"claimed": True, "lease_owner": lease_owner, "data": data}

    return _claim(transaction, doc_ref)


def get_checkpoint_status(data: dict, checkpoint_name: str) -> str | None:
    checkpoint = (data.get("worker_checkpoints") or {}).get(checkpoint_name) or {}
    return checkpoint.get("status")


def update_checkpoint_state(
    doc_ref,
    *,
    lease_owner: str,
    checkpoint_name: str,
    status: str,
    error_message: str | None = None,
):
    transaction = db_client.transaction()

    @firestore.transactional
    def _update(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            raise WorkerLeaseLost(f"Submission disappeared while updating checkpoint {checkpoint_name}")

        data = doc.to_dict()

        if data.get("status") != "processing" or data.get("worker_lease_owner") != lease_owner:
            raise WorkerLeaseLost(f"Worker lease lost while updating checkpoint {checkpoint_name}")

        now = datetime.now(timezone.utc)
        updates = {
            f"worker_checkpoints.{checkpoint_name}.status": status,
            f"worker_checkpoints.{checkpoint_name}.updated_at": now,
            "updated_at": now,
        }

        if status == CHECKPOINT_STATUS_STARTED:
            updates[f"worker_checkpoints.{checkpoint_name}.started_at"] = now
            updates[f"worker_checkpoints.{checkpoint_name}.error_message"] = None

        if status == CHECKPOINT_STATUS_COMPLETED:
            updates[f"worker_checkpoints.{checkpoint_name}.completed_at"] = now
            updates[f"worker_checkpoints.{checkpoint_name}.error_message"] = None

        if status == CHECKPOINT_STATUS_FAILED:
            updates[f"worker_checkpoints.{checkpoint_name}.error_message"] = error_message

        transaction.update(doc_ref, updates)

    _update(transaction, doc_ref)


def run_checkpointed_side_effect(
    doc_ref,
    *,
    lease_owner: str,
    checkpoint_name: str,
    request_id: str,
    submission_id: str,
    submission_type: str,
    callback,
):
    snapshot = doc_ref.get()

    if snapshot.exists and get_checkpoint_status(snapshot.to_dict(), checkpoint_name) == CHECKPOINT_STATUS_COMPLETED:
        log_info(
            "worker_checkpoint_skipped",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            checkpoint=checkpoint_name,
            reason="completed",
        )
        return

    update_checkpoint_state(
        doc_ref,
        lease_owner=lease_owner,
        checkpoint_name=checkpoint_name,
        status=CHECKPOINT_STATUS_STARTED,
    )
    log_info(
        "worker_checkpoint_started",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
        checkpoint=checkpoint_name,
    )

    try:
        callback()
    except Exception as exc:
        update_checkpoint_state(
            doc_ref,
            lease_owner=lease_owner,
            checkpoint_name=checkpoint_name,
            status=CHECKPOINT_STATUS_FAILED,
            error_message=str(exc),
        )
        log_warning(
            "worker_checkpoint_failed",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            checkpoint=checkpoint_name,
            error_message=str(exc),
        )
        raise

    update_checkpoint_state(
        doc_ref,
        lease_owner=lease_owner,
        checkpoint_name=checkpoint_name,
        status=CHECKPOINT_STATUS_COMPLETED,
    )
    log_info(
        "worker_checkpoint_completed",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
        checkpoint=checkpoint_name,
    )


def required_checkpoints_completed(doc_ref, submission_type: str) -> bool:
    snapshot = doc_ref.get()

    if not snapshot.exists:
        return False

    data = snapshot.to_dict()
    return all(get_checkpoint_status(data, checkpoint_name) == CHECKPOINT_STATUS_COMPLETED for checkpoint_name in get_required_checkpoints(submission_type))


def mark_submission_completed(doc_ref, lease_owner: str) -> bool:
    transaction = db_client.transaction()

    @firestore.transactional
    def _complete(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            return False

        data = doc.to_dict()

        if data.get("status") != "processing" or data.get("worker_lease_owner") != lease_owner:
            return False

        now = datetime.now(timezone.utc)
        transaction.update(
            doc_ref,
            {
                "status": "completed",
                "worker_lease_owner": None,
                "worker_lease_expires_at": None,
                "updated_at": now,
            },
        )
        return True

    return _complete(transaction, doc_ref)


def mark_submission_failed(doc_ref, lease_owner: str | None, error_message: str) -> bool:
    transaction = db_client.transaction()

    @firestore.transactional
    def _fail(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            return False

        data = doc.to_dict()

        if lease_owner is not None and (data.get("status") != "processing" or data.get("worker_lease_owner") != lease_owner):
            return False

        now = datetime.now(timezone.utc)
        transaction.update(
            doc_ref,
            {
                "status": "failed",
                "error_message": error_message,
                "worker_lease_owner": None,
                "worker_lease_expires_at": None,
                "updated_at": now,
            },
        )
        return True

    return _fail(transaction, doc_ref)


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
    lease_owner = None

    try:
        claim_result = claim_submission(doc_ref)

        if not claim_result["claimed"]:
            if claim_result["reason"] == "processing":
                log_warning(
                    "submission_processing_lease_active",
                    request_id=request_id,
                    submission_id=submission_id,
                    submission_type=submission_type,
                )
                raise WorkerLeaseActive("Submission is still actively leased by another worker")

            log_info(
                "submission_skipped",
                request_id=request_id,
                submission_id=submission_id,
                submission_type=submission_type,
                reason=claim_result["reason"],
            )
            return

        lease_owner = claim_result["lease_owner"]
        data = doc_ref.get().to_dict()
        submission_type = data.get("type")
        log_info(
            "submission_type_detected",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )

        if submission_type == "delegacion":
            process_delegacion_submission(data, request_id, submission_id, doc_ref, lease_owner)
        else:
            process_faculty_submission(data, request_id, submission_id, doc_ref, lease_owner)

        if not required_checkpoints_completed(doc_ref, submission_type):
            raise RuntimeError("Required worker checkpoints did not all complete")

        if not mark_submission_completed(doc_ref, lease_owner):
            raise WorkerLeaseLost("Worker lease lost before marking submission completed")

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

    except WorkerLeaseActive:
        raise

    except WorkerLeaseLost:
        log_warning(
            "submission_processing_lease_lost",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )
        raise

    except Exception as exc:
        log_exception(
            "submission_processing_failed",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )
        if not mark_submission_failed(doc_ref, lease_owner, str(exc)):
            log_warning(
                "submission_failed_after_lease_lost",
                request_id=request_id,
                submission_id=submission_id,
                submission_type=submission_type,
            )
