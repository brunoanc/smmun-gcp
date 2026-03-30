from datetime import datetime, timedelta, timezone
import json
import logging
import os
import traceback
from uuid import uuid4
import functions_framework
from google.auth import default
from google.cloud import firestore, pubsub_v1


# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
logger = logging.getLogger(__name__)
LOG_COMPONENT = "outbox"


def _to_json_log(event: str, severity: str, **fields) -> str:
    request_id = fields.get("request_id")
    payload = {
        "severity": severity,
        "event": event,
        "component": LOG_COMPONENT,
        **fields,
    }
    if request_id:
        payload["logging.googleapis.com/trace"] = (
            f"projects/{PROJECT_ID}/traces/{request_id}"
        )
    return json.dumps(payload, ensure_ascii=False, default=str)


def log_info(event: str, **fields):
    logger.info(_to_json_log(event, "INFO", **fields))


def log_warning(event: str, **fields):
    logger.warning(_to_json_log(event, "WARNING", **fields))


def log_exception(event: str, **fields):
    logger.error(_to_json_log(event, "ERROR", error=traceback.format_exc(), **fields))


google_credentials, PROJECT_ID = default()

OUTBOX_COLLECTION_NAME = os.getenv("OUTBOX_COLLECTION_NAME", "inscripciones_outbox")

# Keep the lease aligned with the function timeout so crashed publishes are reclaimable quickly.
OUTBOX_PUBLISH_LEASE_SECONDS = 60
OUTBOX_SWEEP_BATCH_SIZE = 50
PUB_SUB_TOPIC_NAME = os.environ["PUB_SUB_TOPIC_NAME"]

db_client = firestore.Client()
outbox_collection = db_client.collection(OUTBOX_COLLECTION_NAME)
publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(PROJECT_ID, PUB_SUB_TOPIC_NAME)


# Normalize CloudEvent payloads that may arrive as decoded JSON or raw bytes
def parse_cloud_event_data(cloud_event) -> dict:
    data = cloud_event.data

    if isinstance(data, dict):
        return data

    if isinstance(data, (bytes, bytearray)):
        try:
            return json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}

    if isinstance(data, str):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return {}

    return {}


def claim_outbox_entry(doc_ref):
    transaction = db_client.transaction()

    @firestore.transactional
    def _claim(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)
        if not doc.exists:
            return {"result": "skip", "reason": "missing"}

        data = doc.to_dict()
        status = data.get("status")
        if status == "sent":
            return {"result": "skip", "reason": "sent"}

        now = datetime.now(timezone.utc)
        publish_started_at = data.get("publish_started_at")
        publishing_is_stale = False
        if publish_started_at is not None:
            publishing_is_stale = (
                now - publish_started_at.astimezone(timezone.utc)
            ).total_seconds() >= OUTBOX_PUBLISH_LEASE_SECONDS

        if status == "publishing" and not publishing_is_stale:
            return {"result": "busy"}

        publisher_request_id = str(uuid4())

        # Only the active lease owner can finalize the row as sent or pending.
        transaction.update(
            doc_ref,
            {
                "status": "publishing",
                "publisher_request_id": publisher_request_id,
                "publish_started_at": now,
                "publish_lease_expires_at": now
                + timedelta(seconds=OUTBOX_PUBLISH_LEASE_SECONDS),
                "attempt_count": firestore.Increment(1),
                "updated_at": now,
            },
        )
        data["outbox_id"] = doc.id
        data["publisher_request_id"] = publisher_request_id
        return {"result": "claimed", "entry": data}

    return _claim(transaction, doc_ref)


def publish_outbox_entry(doc_ref, entry: dict):
    request_id = entry.get("request_id") or str(uuid4())
    submission_id = entry["submission_id"]
    submission_type = entry.get("submission_type")
    publisher_request_id = entry.get("publisher_request_id")

    log_info(
        "outbox_publish_started",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
        outbox_id=entry.get("outbox_id"),
    )

    try:
        publisher_client.publish(
            topic_path,
            json.dumps(
                {
                    "submission_id": submission_id,
                    "request_id": request_id,
                    "submission_type": submission_type,
                }
            ).encode("utf-8"),
        ).result()
    except Exception as exc:
        log_exception(
            "outbox_publish_failed",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            outbox_id=entry.get("outbox_id"),
        )

        if not finalize_outbox_entry(
            doc_ref,
            publisher_request_id,
            {
                "status": "pending",
                "last_error": str(exc),
                "publisher_request_id": None,
                "publish_started_at": None,
                "publish_lease_expires_at": None,
                "updated_at": datetime.now(timezone.utc),
            },
        ):
            log_warning(
                "outbox_publish_failed_after_lease_lost",
                request_id=request_id,
                submission_id=submission_id,
                submission_type=submission_type,
                outbox_id=entry.get("outbox_id"),
            )
            return

        raise

    if not finalize_outbox_entry(
        doc_ref,
        publisher_request_id,
        {
            "status": "sent",
            "last_error": "",
            "publisher_request_id": None,
            "publish_started_at": None,
            "publish_lease_expires_at": None,
            "updated_at": datetime.now(timezone.utc),
        },
    ):
        log_warning(
            "outbox_publish_succeeded_after_lease_lost",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            outbox_id=entry.get("outbox_id"),
        )
        return
    log_info(
        "outbox_publish_succeeded",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
        outbox_id=entry.get("outbox_id"),
    )


def finalize_outbox_entry(doc_ref, publisher_request_id: str | None, updates: dict) -> bool:
    transaction = db_client.transaction()

    @firestore.transactional
    def _finalize(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            return False

        data = doc.to_dict()

        # A newer claim already owns this row, so this invocation must not overwrite it.
        if (
            data.get("status") != "publishing"
            or data.get("publisher_request_id") != publisher_request_id
        ):
            return False

        transaction.update(doc_ref, updates)
        return True

    return _finalize(transaction, doc_ref)


# Shared path for sweep retries and create-trigger deliveries.
def process_outbox_doc(doc):
    claim_result = claim_outbox_entry(doc.reference)

    if claim_result["result"] == "skip":
        log_info(
            "outbox_entry_skipped",
            outbox_id=doc.id,
            reason=claim_result["reason"],
        )
        return

    if claim_result["result"] == "busy":
        log_info("outbox_entry_busy", outbox_id=doc.id, reason="publishing")
        return

    publish_outbox_entry(doc.reference, claim_result["entry"])


def run_outbox_sweep():
    pending_docs = (
        outbox_collection.where("status", "==", "pending")
        .order_by("updated_at")
        .limit(OUTBOX_SWEEP_BATCH_SIZE)
        .stream()
    )

    # The sweep only recovers pending rows and stale leases that the create trigger did not finish.
    for doc in pending_docs:
        try:
            process_outbox_doc(doc)
        except Exception:
            log_exception("outbox_sweep_row_failed", outbox_id=doc.id)

    publishing_docs = (
        outbox_collection.where("status", "==", "publishing")
        .where("publish_lease_expires_at", "<=", datetime.now(timezone.utc))
        .order_by("publish_lease_expires_at")
        .limit(OUTBOX_SWEEP_BATCH_SIZE)
        .stream()
    )
    for doc in publishing_docs:
        try:
            process_outbox_doc(doc)
        except Exception:
            log_exception("outbox_sweep_row_failed", outbox_id=doc.id)


# Publish immediately when a new outbox row is created.
@functions_framework.cloud_event
def outbox_created_handler(cloud_event):
    event_data = parse_cloud_event_data(cloud_event)
    document_name = event_data.get("value", {}).get("name")

    if not document_name:
        log_warning("outbox_event_missing_document_name")
        return

    outbox_id = document_name.split("/")[-1]
    doc_ref = outbox_collection.document(outbox_id)
    doc = doc_ref.get()
    if not doc.exists:
        log_info("outbox_entry_skipped", outbox_id=outbox_id, reason="missing")
        return

    claim_result = claim_outbox_entry(doc_ref)
    if claim_result["result"] == "skip":
        log_info(
            "outbox_entry_skipped",
            outbox_id=outbox_id,
            reason=claim_result["reason"],
        )
        return

    if claim_result["result"] == "busy":
        log_info("outbox_entry_busy", outbox_id=outbox_id, reason="publishing")
        raise RuntimeError(f"Outbox row {outbox_id} is still publishing")

    publish_outbox_entry(doc_ref, claim_result["entry"])


# Safety net for pending rows and leases that were never finalized.
@functions_framework.cloud_event
def outbox_sweep_handler(cloud_event):
    log_info("outbox_sweep_started")
    run_outbox_sweep()
    log_info("outbox_sweep_finished")
