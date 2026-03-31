"""GCP/runtime helpers for the API: app setup, logging, redirects, idempotency, and persistence."""

from datetime import datetime, timezone
from fastapi import APIRouter, FastAPI, Request, UploadFile, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from google.auth import default
from google.cloud import firestore, storage
from starlette.exceptions import HTTPException as StarletteHTTPException
from time import perf_counter
from urllib.parse import urlparse
from uuid import uuid4
import hashlib
import json
import logging
import os
import threading
import traceback

# Frontend redirect settings
URL_BASE = "https://smmun.com"
CONFIRMATION_ROUTE_PATH = "/registro/confirmacion/"
ERROR_ROUTE_PATH = "/registro/error/"
ALLOWED_REDIRECT_ORIGINS = {
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:5173",
    "https://smmun.com",
    "https://www.smmun.com",
    "https://smmun0githubio-production.up.railway.app",
    "https://smmun0.github.io",
    "https://github.io",
}


# GCP resource names
COMPROBANTES_BUCKET_NAME = os.environ["COMPROBANTES_BUCKET_NAME"]
FIRESTORE_COLLECTION_NAME = os.environ["FIRESTORE_COLLECTION_NAME"]
IDEMPOTENCY_COLLECTION_NAME = os.getenv("IDEMPOTENCY_COLLECTION_NAME", "inscripciones_idempotencia")
OUTBOX_COLLECTION_NAME = os.getenv("OUTBOX_COLLECTION_NAME", "inscripciones_outbox")
_, PROJECT_ID = default()


# Logging and request metadata
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
logger = logging.getLogger(__name__)
REQUEST_ID_HEADER = "X-Request-ID"
IDEMPOTENCY_KEY_FIELD = "idempotency_key"
LOG_COMPONENT = "api"
SAFE_CONTEXT_KEYS = {
    "comite",
    "modalidad",
    "tipo",
    "content_type",
    "size",
    "esperado",
    "recibido",
    "numero_delegaciones",
    "idempotency_status",
}


# Idempotency timing
IDEMPOTENCY_RECOVERY_WINDOW_SECONDS = 300
IDEMPOTENCY_CLAIM_RECOVERY_SECONDS = 30
IDEMPOTENCY_CLAIM_HEARTBEAT_SECONDS = 10


# Cloud Storage backend
storage_client = storage.Client()
comprobantes_bucket = storage_client.bucket(COMPROBANTES_BUCKET_NAME)


# Firestore backend
db_client = firestore.Client()
db_collection = db_client.collection(FIRESTORE_COLLECTION_NAME)
idempotency_collection = db_client.collection(IDEMPOTENCY_COLLECTION_NAME)
outbox_collection = db_client.collection(OUTBOX_COLLECTION_NAME)


# FastAPI app
router = APIRouter()
app = FastAPI(docs_url=None, redoc_url=None)


# Sanitize validation context before it is logged
def _sanitize_context_value(value):
    if value is None or isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, str):
        return {"present": bool(value.strip()), "length": len(value)}

    if isinstance(value, list):
        return {"count": len(value), "unique_count": len({str(v) for v in value})}

    if isinstance(value, dict):
        return {k: _sanitize_context_value(v) for k, v in value.items()}

    return {"type": type(value).__name__}


# Map the backend submission type to the frontend form name
def get_form_name(submission_type: str) -> str:
    return "delegaciones" if submission_type == "delegacion" else "faculty"


# Build a frontend redirect URL from an origin and path
def build_frontend_redirect(origin: str, path: str) -> str:
    return f"{origin.rstrip('/')}{path}"


# Accept only redirect origins that are explicitly allowed
def normalize_allowed_origin(candidate: str | None) -> str | None:
    if not candidate:
        return None

    parsed = urlparse(candidate)

    if not parsed.scheme or not parsed.netloc:
        return None

    origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

    if origin in ALLOWED_REDIRECT_ORIGINS:
        return origin

    return None


# Resolve the frontend origin from request headers with a safe fallback
def get_frontend_origin(request: Request) -> str:
    return normalize_allowed_origin(request.headers.get("origin")) or normalize_allowed_origin(request.headers.get("referer")) or URL_BASE


# Build the confirmation redirect URL for a submission type
def get_confirmation_redirect(submission_type: str, frontend_origin: str) -> str:
    return f"{build_frontend_redirect(frontend_origin, CONFIRMATION_ROUTE_PATH)}?form={get_form_name(submission_type)}"


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


# Log sanitized validation context and raise a user-facing error
def raise_validation_error(request_id: str, message: str, **context):
    sanitized_context = {}

    for key, value in context.items():
        if key in SAFE_CONTEXT_KEYS:
            sanitized_context[key] = value
        else:
            sanitized_context[key] = _sanitize_context_value(value)

    log_warning(
        "validation_failed",
        request_id=request_id,
        reason=message,
        context=sanitized_context,
    )
    raise ValueError(message)


# Attach or generate a request ID and log the request lifecycle
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
    request.state.request_id = request_id
    start = perf_counter()

    log_info(
        "request_started",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
    )

    try:
        response = await call_next(request)
    except Exception:
        duration_ms = int((perf_counter() - start) * 1000)
        log_exception(
            "request_failed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            duration_ms=duration_ms,
        )
        raise

    duration_ms = int((perf_counter() - start) * 1000)
    response.headers[REQUEST_ID_HEADER] = request_id
    log_info(
        "request_finished",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=duration_ms,
    )
    return response


# Redirect validation and HTTP errors to the frontend error page
@app.exception_handler(StarletteHTTPException)
@app.exception_handler(RequestValidationError)
@app.exception_handler(ValueError)
async def http_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    frontend_origin = get_frontend_origin(request)
    log_warning(
        "request_redirected_to_error",
        request_id=request_id,
        path=request.url.path,
        exception_type=type(exc).__name__,
    )
    return RedirectResponse(
        build_frontend_redirect(frontend_origin, ERROR_ROUTE_PATH),
        status_code=status.HTTP_303_SEE_OTHER,
    )


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://localhost:8080",
        "http://localhost:5173",
        "https://smmun.com",
        "https://www.smmun.com",
        "https://smmun0githubio-production.up.railway.app",
        "https://smmun0.github.io",
        "https://github.io",
    ],
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["*"],
)


# Validate and normalize an explicit idempotency key from the client
def validate_idempotency_key(request_id: str, key: str | None) -> str | None:
    if key is None or not key.strip():
        return None

    normalized_key = key.strip()

    if len(normalized_key) > 128:
        raise_validation_error(request_id, "La llave de idempotencia es inválida.")

    allowed_characters = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")

    if any(ch not in allowed_characters for ch in normalized_key):
        raise_validation_error(request_id, "La llave de idempotencia es inválida.")

    return normalized_key


# Build the Firestore document ID for an idempotency record
def build_idempotency_doc_id(submission_type: str, key: str) -> str:
    return f"{submission_type}:{key}"


# Build the primary outbox document ID for an initial submission publish
def build_outbox_doc_id(submission_id: str) -> str:
    return submission_id


class LostIdempotencyClaimError(Exception):
    pass


# Claim, replay, or reject an idempotency key transactionally
def claim_idempotency_key(
    submission_type: str,
    key: str,
    payload_hash: str,
    request_id: str,
) -> dict:
    transaction = db_client.transaction()
    doc_ref = idempotency_collection.document(build_idempotency_doc_id(submission_type, key))
    now = datetime.now(timezone.utc)

    # Claim an idempotency record if it is new or recoverable
    @firestore.transactional
    def _claim(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)

        if not snapshot.exists:
            transaction.set(
                doc_ref,
                {
                    "submission_type": submission_type,
                    IDEMPOTENCY_KEY_FIELD: key,
                    "payload_hash": payload_hash,
                    "request_id": request_id,
                    "status": "claimed",
                    "submission_id": None,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return {"result": "claimed", "recovered": False}

        record = snapshot.to_dict()

        if record.get("payload_hash") != payload_hash:
            return {"result": "conflict", "status": record.get("status")}

        record_status = record.get("status")
        submission_id = record.get("submission_id")
        updated_at = record.get("updated_at")
        is_stale = False
        claim_is_stale = False

        if updated_at is not None:
            updated_at_utc = updated_at.astimezone(timezone.utc)
            is_stale = (now - updated_at_utc).total_seconds() >= IDEMPOTENCY_RECOVERY_WINDOW_SECONDS
            claim_is_stale = (now - updated_at_utc).total_seconds() >= IDEMPOTENCY_CLAIM_RECOVERY_SECONDS

        if submission_id is not None:
            if record_status != "committed":
                transaction.update(
                    doc_ref,
                    {
                        "status": "committed",
                        "updated_at": now,
                    },
                )

            return {
                "result": "replay",
                "status": "committed",
                "submission_id": submission_id,
            }

        if record_status == "claimed":
            if claim_is_stale:
                transaction.update(
                    doc_ref,
                    {
                        "request_id": request_id,
                        "updated_at": now,
                    },
                )
                return {"result": "claimed", "recovered": True}

            return {"result": "in_progress", "status": "claimed"}

        if is_stale:
            transaction.set(
                doc_ref,
                {
                    "submission_type": submission_type,
                    IDEMPOTENCY_KEY_FIELD: key,
                    "payload_hash": payload_hash,
                    "request_id": request_id,
                    "status": "claimed",
                    "submission_id": None,
                    "updated_at": now,
                },
                merge=True,
            )
            return {"result": "claimed", "recovered": True}

        return {"result": "in_progress", "status": record_status}

    return _claim(transaction, doc_ref)


# Delete an uncommitted claim still owned by the current request
def release_claimed_idempotency_key(submission_type: str, key: str, request_id: str) -> None:
    transaction = db_client.transaction()
    doc_ref = idempotency_collection.document(build_idempotency_doc_id(submission_type, key))

    # Delete a claim if this request still owns it and nothing was committed
    @firestore.transactional
    def _release(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)

        if not snapshot.exists:
            return

        record = snapshot.to_dict()

        if record.get("status") == "claimed" and record.get("request_id") == request_id and record.get("submission_id") is None:
            transaction.delete(doc_ref)

    _release(transaction, doc_ref)


# Refresh the lease timestamp for an in-flight claimed idempotency record
def refresh_claimed_idempotency_key(submission_type: str, key: str, request_id: str) -> bool:
    transaction = db_client.transaction()
    doc_ref = idempotency_collection.document(build_idempotency_doc_id(submission_type, key))

    # Renew a claimed idempotency lease if this request still owns it
    @firestore.transactional
    def _refresh(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)

        if not snapshot.exists:
            return False

        record = snapshot.to_dict()

        if record.get("status") != "claimed" or record.get("request_id") != request_id or record.get("submission_id") is not None:
            return False

        transaction.update(doc_ref, {"updated_at": datetime.now(timezone.utc)})
        return True

    return _refresh(transaction, doc_ref)


# Start a background heartbeat that keeps a pre-commit claim fresh
def start_idempotency_claim_heartbeat(submission_type: str, key: str, request_id: str) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    # Refresh the claim lease until the request finishes
    def _heartbeat():
        while not stop_event.wait(IDEMPOTENCY_CLAIM_HEARTBEAT_SECONDS):
            try:
                if not refresh_claimed_idempotency_key(submission_type, key, request_id):
                    return
            except Exception:
                log_exception(
                    "idempotency_claim_heartbeat_failed",
                    request_id=request_id,
                    submission_type=submission_type,
                )

    thread = threading.Thread(target=_heartbeat, daemon=True)
    thread.start()
    return stop_event, thread


# Stop the background heartbeat used for claim lease renewal
def stop_idempotency_claim_heartbeat(stop_event: threading.Event | None, thread: threading.Thread | None) -> None:
    if stop_event is None or thread is None:
        return

    stop_event.set()
    thread.join(timeout=1)


# Re-read durable state after an ambiguous commit failure
def inspect_persist_submission_outcome(
    *,
    submission_ref,
    submission_type: str,
    idempotency_key: str,
    request_id: str,
) -> str:
    idempotency_ref = idempotency_collection.document(build_idempotency_doc_id(submission_type, idempotency_key))
    outbox_ref = outbox_collection.document(build_outbox_doc_id(submission_ref.id))

    submission_snapshot = submission_ref.get()
    idempotency_snapshot = idempotency_ref.get()
    outbox_snapshot = outbox_ref.get()

    if submission_snapshot.exists and idempotency_snapshot.exists and outbox_snapshot.exists:
        idempotency_record = idempotency_snapshot.to_dict()

        if idempotency_record.get("status") == "committed" and idempotency_record.get("submission_id") == submission_ref.id:
            return "committed"

    if not submission_snapshot.exists and not outbox_snapshot.exists:
        if idempotency_snapshot.exists:
            idempotency_record = idempotency_snapshot.to_dict()

            if (
                idempotency_record.get("status") == "claimed"
                and idempotency_record.get("request_id") == request_id
                and idempotency_record.get("submission_id") is None
            ):
                return "not_committed"

        if not idempotency_snapshot.exists:
            return "not_committed"

    return "unknown"


# Atomically write the submission, outbox row, and committed idempotency record
def persist_submission_idempotency_and_outbox(
    *,
    submission_ref,
    submission_payload: dict,
    outbox_payload: dict,
    submission_type: str,
    idempotency_key: str,
    request_id: str,
):
    idempotency_ref = idempotency_collection.document(build_idempotency_doc_id(submission_type, idempotency_key))
    outbox_ref = outbox_collection.document(build_outbox_doc_id(submission_ref.id))
    transaction = db_client.transaction()

    # Persist the submission and finalize the idempotency record in one transaction
    @firestore.transactional
    def _persist(transaction, idempotency_ref, submission_ref, outbox_ref):
        snapshot = idempotency_ref.get(transaction=transaction)

        if not snapshot.exists:
            raise LostIdempotencyClaimError()

        record = snapshot.to_dict()

        if record.get("status") != "claimed" or record.get("request_id") != request_id or record.get("submission_id") is not None:
            raise LostIdempotencyClaimError()

        now = datetime.now(timezone.utc)
        transaction.set(submission_ref, submission_payload)
        transaction.set(outbox_ref, outbox_payload)
        transaction.set(
            idempotency_ref,
            {
                "status": "committed",
                "request_id": request_id,
                "updated_at": now,
                "submission_id": submission_ref.id,
            },
            merge=True,
        )

    _persist(transaction, idempotency_ref, submission_ref, outbox_ref)


# Read the current processing status of an existing submission
def get_submission_status(submission_id: str) -> str | None:
    snapshot = db_collection.document(submission_id).get()

    if not snapshot.exists:
        return None

    return snapshot.to_dict().get("status")


# Reopen a payload-hash fallback claim so legacy clients can retry after failure
def reopen_failed_fallback_idempotency_key(
    submission_type: str,
    key: str,
    request_id: str,
    failed_submission_id: str,
) -> bool:
    transaction = db_client.transaction()
    doc_ref = idempotency_collection.document(build_idempotency_doc_id(submission_type, key))

    # Reopen a fallback idempotency claim for a failed submission
    @firestore.transactional
    def _reopen(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)

        if not snapshot.exists:
            return False

        record = snapshot.to_dict()

        if record.get("status") != "committed" or record.get("submission_id") != failed_submission_id:
            return False

        transaction.update(
            doc_ref,
            {
                "status": "claimed",
                "request_id": request_id,
                "submission_id": None,
                "updated_at": datetime.now(timezone.utc),
            },
        )
        return True

    return _reopen(transaction, doc_ref)


# Hash the uploaded file bytes without consuming the stream permanently
async def get_upload_file_hash(upload: UploadFile) -> str:
    sha256 = hashlib.sha256()
    upload.file.seek(0)

    while True:
        chunk = await upload.read(1024 * 1024)

        if not chunk:
            break

        sha256.update(chunk)

    upload.file.seek(0)
    return sha256.hexdigest()


# Build an error redirect that tells the frontend whether to rotate the key
def error_redirect(
    submission_type: str,
    frontend_origin: str,
    *,
    rotate_idempotency_key: bool = False,
) -> RedirectResponse:
    query = (
        f"?rotate_idempotency_key=1&form={get_form_name(submission_type)}"
        if rotate_idempotency_key
        else f"?keep_idempotency_key=1&form={get_form_name(submission_type)}"
    )
    return RedirectResponse(
        f"{build_frontend_redirect(frontend_origin, ERROR_ROUTE_PATH)}{query}",
        status_code=status.HTTP_303_SEE_OTHER,
    )
