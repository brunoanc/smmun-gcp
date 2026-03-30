from typing import Annotated, Optional, cast
from fastapi import FastAPI, APIRouter, Form, File, UploadFile, Depends, status, Request
from fastapi.responses import RedirectResponse
from pydantic import EmailStr
from dataclasses import dataclass, fields as dataclass_fields
import logging
import unicodedata
from starlette.datastructures import FormData
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
from google.cloud import storage, firestore
from google.auth import default
from time import perf_counter
from uuid import uuid4
from urllib.parse import urlparse
import traceback
import os
import json
import mimetypes
import hashlib
import threading

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


# Idempotency and publish timing
IDEMPOTENCY_RECOVERY_WINDOW_SECONDS = 300
IDEMPOTENCY_CLAIM_RECOVERY_SECONDS = 30
IDEMPOTENCY_CLAIM_HEARTBEAT_SECONDS = 10
OUTBOX_PUBLISH_LEASE_SECONDS = 60


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
    return f"{build_frontend_redirect(frontend_origin, CONFIRMATION_ROUTE_PATH)}" f"?form={get_form_name(submission_type)}"


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


# Allowed committees and misc delegation types
COMITES_VALIDOS = [
    "SOCHUM",
    "ONU SIDA",
    "ONU-Hábitat",
    "CCPCJ",
    "UNRWA",
    "Cumbre",
    "NASA",
    "WWF",
    "Crisis",
    "FIA",
    "FHCM",

]
TIPOS_SOLO_INDIVIDUAL = {
    "pilotos",
    "disenadores_emergentes",
    "astronautas",
    "representantes_nasa",

}
COMITES_CON_TIPOS = {"fia", "fhcm", "nasa", "cumbre_futuro"}


# All valid delegations
with open("delegaciones.json", "r", encoding="utf-8") as delegaciones_json:
    delegaciones_data = json.load(delegaciones_json)


@dataclass
class DelegacionFormData:
    idempotency_key: Optional[str] = Form(None, max_length=128)
    modalidad: str = Form(pattern=r"^(individual|pareja)$")
    delegacion_oficial: str = Form(pattern=r"^(si|no)$")
    nombre_delegacion_oficial: Optional[str] = Form(None, max_length=150)
    responsable_delegacion_oficial: Optional[str] = Form(None, max_length=150)

    nombre_0: str = Form(max_length=150)
    apellido_0: str = Form(max_length=150)
    edad_0: str = Form(max_length=2)
    celular_0: str = Form(max_length=30)
    correo_0: EmailStr = Form(max_length=150)
    pais_0: str = Form(max_length=150)
    ciudad_estado_0: str = Form(max_length=150)
    escolaridad_0: str = Form(
        pattern=r"^(Secundaria|Preparatoria|Universidad|Egresado|No estudio)$",
        max_length=150,

    )
    escuela_0: Optional[str] = Form(None, max_length=150)
    nombre_contacto_0: str = Form(max_length=150)
    celular_contacto_0: str = Form(max_length=30)
    relacion_contacto_0: str = Form(max_length=150)
    info_extra_0: Optional[str] = Form(None, max_length=150)

    nombre_1: Optional[str] = Form(None, max_length=150)
    apellido_1: Optional[str] = Form(None, max_length=150)
    edad_1: Optional[str] = Form(None, max_length=2)
    celular_1: Optional[str] = Form(None, max_length=30)
    correo_1: Optional[EmailStr | str] = Form(None, max_length=150)
    pais_1: Optional[str] = Form(None, max_length=150)
    ciudad_estado_1: Optional[str] = Form(None, max_length=150)
    escolaridad_1: Optional[str] = Form(
        None,
        pattern=r"^(|Secundaria|Preparatoria|Universidad|Egresado|No estudio)$",
        max_length=150,

    )
    escuela_1: Optional[str] = Form(None, max_length=150)
    nombre_contacto_1: Optional[str] = Form(None, max_length=150)
    celular_contacto_1: Optional[str] = Form(None, max_length=30)
    relacion_contacto_1: Optional[str] = Form(None, max_length=150)
    info_extra_1: Optional[str] = Form(None, max_length=150)

    comite_0: str = Form(pattern=r"^(SOCHUM|ONU SIDA|ONU-Hábitat|CCPCJ|UNRWA|Cumbre|NASA|WWF|Crisis|FIA|FHCM)$")
    comite_0_pais_0: str = Form(max_length=150)
    comite_0_pais_1: str = Form(max_length=150)
    comite_0_pais_2: Optional[str] = Form(None, max_length=150)

    comite_1: str = Form(pattern=r"^(SOCHUM|ONU SIDA|ONU-Hábitat|CCPCJ|UNRWA|Cumbre|NASA|WWF|Crisis|FIA|FHCM)$")
    comite_1_pais_0: str = Form(max_length=150)
    comite_1_pais_1: str = Form(max_length=150)
    comite_1_pais_2: Optional[str] = Form(None, max_length=150)

    comite_2: str = Form(pattern=r"^(SOCHUM|ONU SIDA|ONU-Hábitat|CCPCJ|UNRWA|Cumbre|NASA|WWF|Crisis|FIA|FHCM)$")
    comite_2_pais_0: str = Form(max_length=150)
    comite_2_pais_1: str = Form(max_length=150)
    comite_2_pais_2: Optional[str] = Form(None, max_length=150)


@dataclass
class FacultyFormData:
    idempotency_key: Optional[str] = Form(None, max_length=128)
    institucion_delegacion_oficial: str = Form(max_length=150)
    nombre_faculty: str = Form(max_length=150)
    apellido_faculty: str = Form(max_length=150)
    celular_faculty: str = Form(max_length=150)
    correo_faculty: EmailStr = Form(max_length=150)
    ciudad_estado_faculty: str = Form(max_length=150)
    pais_faculty: str = Form(max_length=150)
    numero_delegaciones: str = Form(max_length=2)


# Cloud Storage backend
storage_client = storage.Client()
comprobantes_bucket = storage_client.bucket(COMPROBANTES_BUCKET_NAME)


# Firestore backend
db_client = firestore.Client()
db_collection = db_client.collection(FIRESTORE_COLLECTION_NAME)
idempotency_collection = db_client.collection(IDEMPOTENCY_COLLECTION_NAME)
outbox_collection = db_client.collection(OUTBOX_COLLECTION_NAME)


router = APIRouter()
app = FastAPI(docs_url=None, redoc_url=None)


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


# Normalize a committee code for internal lookups
def normalizar_comite(siglas: str) -> str:
    texto = unicodedata.normalize("NFD", siglas)
    texto = "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")
    texto = texto.lower().replace(" ", "_").replace("-", "_")

    if texto == "cumbre":
        return "cumbre_futuro"

    return texto


# Resolve the delegation type for a committee and selection name
def obtener_tipo_delegacion(comite_siglas: str, delegacion_nombre: str) -> Optional[str]:
    clave = normalizar_comite(comite_siglas)
    data = delegaciones_data.get(clave)

    if not isinstance(data, dict):
        return None

    for tipo, lista in data.items():
        if any(item.get("nombre") == delegacion_nombre for item in lista):
            return tipo

    return None


# Split a committee/delegation composite value
def parse_delegacion(valor: str) -> tuple[str, str]:
    if ":" not in valor:
        raise ValueError("Delegación inválida.")

    comite_valor, delegacion = valor.split(":", 1)
    return comite_valor, delegacion


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


# Parse faculty delegation rows from the submitted form
def parse_delegaciones_faculty(form, count: int):
    delegaciones = []

    for i in range(count):
        nombre = form.get(f"nombre_d{i}")
        apellido = form.get(f"apellido_d{i}")
        edad = form.get(f"edad_d{i}")
        celular = form.get(f"celular_d{i}")
        correo = form.get(f"correo_d{i}")
        pais = form.get(f"pais_d{i}")
        ciudad_estado = form.get(f"ciudad_estado_d{i}")
        escolaridad = form.get(f"escolaridad_d{i}")
        escuela = form.get(f"escuela_d{i}") or "No aplica"

        # Skip completely empty rows.
        if not nombre and not apellido:
            continue

        delegaciones.append(
            {
                "nombre": nombre,
                "apellido": apellido,
                "edad": int(edad) if edad else None,
                "celular": celular,
                "correo": correo,
                "pais": pais,
                "ciudad_estado": ciudad_estado,
                "escolaridad": escolaridad,
                "escuela": escuela,
            }
        )

    return delegaciones


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


# Derive a deterministic payload hash for fallback idempotency
def get_payload_hash(
    data,
    submission_type: str,
    file_hash: str,
    extra_payload: dict | None = None,
) -> str:
    payload = {
        "submission_type": submission_type,
        "fields": {field.name: getattr(data, field.name) for field in dataclass_fields(data) if field.name != IDEMPOTENCY_KEY_FIELD},
        "file": {
            "hash": file_hash,
        },

    }

    if extra_payload is not None:
        payload["extra"] = extra_payload

    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# Parse and range-check an age field from the form payload
def parse_age_value(
    request_id: str,
    value: str | None,
    *,
    field_name: str,
    modalidad: str,

) -> int:
    if value is None:
        raise_validation_error(
            request_id,
            "Edad inválida.",
            modalidad=modalidad,
            campo=field_name,
        )

    try:
        parsed = int(value)

    except (TypeError, ValueError):
        raise_validation_error(
            request_id,
            "Edad inválida.",
            modalidad=modalidad,
            campo=field_name,
        )

    if not 11 <= parsed <= 26:
        raise_validation_error(
            request_id,
            "Edad inválida.",
            modalidad=modalidad,
            campo=field_name,
        )

    return parsed


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

    @firestore.transactional
    def _reopen(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)

        if not snapshot.exists:
            return False

        record = snapshot.to_dict()

        if record.get("status") != "committed" or record.get("submission_id") != failed_submission_id:
            return False

        # Legacy clients reuse payload_hash as the key, so reopen the claim for a fresh attempt.
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


# Accept, deduplicate, and persist a delegaciones submission
@router.post("/registro/delegaciones")
async def registrar(
    request: Request,
    data: DelegacionFormData = Depends(),
    comprobante: UploadFile = File(...),

):
    request_id = getattr(request.state, "request_id", str(uuid4()))
    frontend_origin = get_frontend_origin(request)
    submission_type = "delegacion"
    normalized_idempotency_key = validate_idempotency_key(request_id, data.idempotency_key)
    log_info("delegaciones_start", request_id=request_id, modalidad=data.modalidad)

    if comprobante.content_type is None or comprobante.size is None:
        raise_validation_error(request_id, "No se envió la imagen.")

    if not (comprobante.content_type.startswith("image/") or comprobante.content_type == "application/pdf") or comprobante.size > 5242880:
        raise_validation_error(
            request_id,
            "Imagen inválida.",
            content_type=comprobante.content_type,
            size=comprobante.size,
        )

    file_hash = await get_upload_file_hash(comprobante)

    if data.modalidad == "pareja" and (
        data.comite_0 in ["Cumbre", "Crisis"] or data.comite_1 in ["Cumbre", "Crisis"] or data.comite_2 in ["Cumbre", "Crisis"]
    ):
        raise_validation_error(
            request_id,
            "Opción inválida de comité para codelegación.",
            comites=[data.comite_0, data.comite_1, data.comite_2],
        )

    comites = [data.comite_0, data.comite_1, data.comite_2]

    if len(comites) != len(set(comites)):
        raise_validation_error(request_id, "Opciones de comités repetidas.", comites=comites)

    if data.delegacion_oficial == "si" and (not data.nombre_delegacion_oficial or not data.responsable_delegacion_oficial):
        raise_validation_error(request_id, "Faltan datos de delegación oficial.")

    paises_0 = [data.comite_0_pais_0, data.comite_0_pais_1, data.comite_0_pais_2]

    if len(paises_0) != len(set(paises_0)):
        raise_validation_error(
            request_id,
            "Opciones de delegación repetidas.",
            comite=data.comite_0,
            opciones=paises_0,
        )

    paises_1 = [data.comite_1_pais_0, data.comite_1_pais_1, data.comite_1_pais_2]

    if len(paises_1) != len(set(paises_1)):
        raise_validation_error(
            request_id,
            "Opciones de delegación repetidas.",
            comite=data.comite_1,
            opciones=paises_1,
        )

    paises_2 = [data.comite_2_pais_0, data.comite_2_pais_1, data.comite_2_pais_2]

    if len(paises_2) != len(set(paises_2)):
        raise_validation_error(
            request_id,
            "Opciones de delegación repetidas.",
            comite=data.comite_2,
            opciones=paises_2,
        )

    es_codelegacion = data.modalidad == "pareja"
    edad_0 = parse_age_value(
        request_id,
        data.edad_0,
        field_name="edad_0",
        modalidad=data.modalidad,
    )
    edad_1 = (
        parse_age_value(
            request_id,
            cast(str | None, data.edad_1),
            field_name="edad_1",
            modalidad=data.modalidad,

        )
        if es_codelegacion
        else None
    )

    if es_codelegacion:
        requeridos = [
            data.nombre_1,
            data.apellido_1,
            data.edad_1,
            data.celular_1,
            data.correo_1,
            data.pais_1,
            data.ciudad_estado_1,
            data.escolaridad_1,
            data.nombre_contacto_1,
            data.celular_contacto_1,
            data.relacion_contacto_1,
        ]

        if any(item is None or str(item).strip() == "" for item in requeridos):
            raise_validation_error(request_id, "Faltan datos de la codelegación.")

    if es_codelegacion:
        delegaciones_seleccionadas = [
            (data.comite_0, data.comite_0_pais_0),
            (data.comite_0, data.comite_0_pais_1),
            (data.comite_0, data.comite_0_pais_2),
            (data.comite_1, data.comite_1_pais_0),
            (data.comite_1, data.comite_1_pais_1),
            (data.comite_1, data.comite_1_pais_2),
            (data.comite_2, data.comite_2_pais_0),
            (data.comite_2, data.comite_2_pais_1),
            (data.comite_2, data.comite_2_pais_2),
        ]

        for comite_siglas, valor in delegaciones_seleccionadas:
            if not valor:
                continue

            if ":" not in valor:
                raise_validation_error(
                    request_id,
                    "Delegación inválida.",
                    comite=comite_siglas,
                    valor=valor,
                )

            _, delegacion_nombre = parse_delegacion(valor)
            tipo = obtener_tipo_delegacion(comite_siglas, delegacion_nombre)

            if tipo in TIPOS_SOLO_INDIVIDUAL:
                raise_validation_error(
                    request_id,
                    "Delegación no disponible para codelegación.",
                    comite=comite_siglas,
                    delegacion=delegacion_nombre,
                    tipo=tipo,
                )

    payload_hash = get_payload_hash(
        data,
        submission_type,
        file_hash,
    )
    idempotency_key = normalized_idempotency_key or payload_hash
    idempotency_result = claim_idempotency_key(
        submission_type,
        idempotency_key,
        payload_hash,
        request_id,
    )

    if idempotency_result["result"] == "replay":
        replay_submission_id = idempotency_result.get("submission_id")
        replay_submission_status = get_submission_status(replay_submission_id) if replay_submission_id is not None else None

        if replay_submission_id is not None and replay_submission_status == "failed":
            if normalized_idempotency_key is None:
                if not reopen_failed_fallback_idempotency_key(
                    submission_type,
                    idempotency_key,
                    request_id,
                    replay_submission_id,
                ):
                    log_warning(
                        "failed_submission_fallback_retry_unavailable",
                        request_id=request_id,
                        submission_id=replay_submission_id,
                        submission_type=submission_type,
                    )
                    await comprobante.close()
                    return error_redirect(
                        submission_type,
                        frontend_origin,
                        rotate_idempotency_key=False,
                    )

                log_warning(
                    "failed_submission_fallback_retry_reclaimed",
                    request_id=request_id,
                    submission_id=replay_submission_id,
                    submission_type=submission_type,
                )
                idempotency_result = {"result": "claimed", "recovered": True}

            else:
                log_warning(
                    "failed_submission_replay_requires_new_attempt",
                    request_id=request_id,
                    submission_id=replay_submission_id,
                    submission_type=submission_type,
                )
                await comprobante.close()
                return error_redirect(
                    submission_type,
                    frontend_origin,
                    rotate_idempotency_key=True,
                )

        if idempotency_result["result"] == "replay":
            log_info(
                "idempotency_replayed",
                request_id=request_id,
                submission_type=submission_type,
                idempotency_status=idempotency_result.get("status"),
                submission_id=replay_submission_id,
            )
            await comprobante.close()
            return RedirectResponse(
                get_confirmation_redirect(submission_type, frontend_origin),
                status_code=status.HTTP_303_SEE_OTHER,
            )

    if idempotency_result["result"] == "conflict":
        log_warning(
            "idempotency_conflict",
            request_id=request_id,
            submission_type=submission_type,
            idempotency_status=idempotency_result.get("status"),
        )
        await comprobante.close()
        return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=True)

    if idempotency_result["result"] == "in_progress":
        log_warning(
            "idempotency_in_progress",
            request_id=request_id,
            submission_type=submission_type,
            idempotency_status=idempotency_result.get("status"),
        )
        await comprobante.close()
        return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

    log_info(
        ("idempotency_claimed" if not idempotency_result.get("recovered") else "idempotency_claim_recovered"),
        request_id=request_id,
        submission_type=submission_type,
    )
    heartbeat_stop_event, heartbeat_thread = start_idempotency_claim_heartbeat(submission_type, idempotency_key, request_id)

    try:
        delegacion_oficial = {
            "is_oficial": data.delegacion_oficial == "si",
            "nombre": data.nombre_delegacion_oficial or "No aplica",
            "responsable": data.responsable_delegacion_oficial or "No aplica",
        }
        participantes = [
            {
                "nombre": data.nombre_0,
                "apellido": data.apellido_0,
                "edad": edad_0,
                "celular": data.celular_0,
                "correo": data.correo_0,
                "pais": data.pais_0,
                "ciudad_estado": data.ciudad_estado_0,
                "escolaridad": data.escolaridad_0,
                "escuela": data.escuela_0 or "No aplica",
                "contacto_emergencia": f"{data.nombre_contacto_0} ({data.relacion_contacto_0}): {data.celular_contacto_0}",
                "info_extra": data.info_extra_0,
            }
        ]

        if es_codelegacion:
            participantes.append(
                {
                    "nombre": data.nombre_1,
                    "apellido": data.apellido_1,
                    "edad": cast(int, edad_1),
                    "celular": data.celular_1,
                    "correo": data.correo_1,
                    "pais": data.pais_1,
                    "ciudad_estado": data.ciudad_estado_1,
                    "escolaridad": data.escolaridad_1,
                    "escuela": (data.escuela_1 or "No aplica"),
                    "contacto_emergencia": f"{data.nombre_contacto_1} ({data.relacion_contacto_1}): {data.celular_contacto_1}",
                    "info_extra": (data.info_extra_1 if es_codelegacion and data.info_extra_1 else None),
                }
            )

        comites = [
            {
                "nombre": data.comite_0,
                "opciones": [
                    data.comite_0_pais_0.split(":")[1],
                    data.comite_0_pais_1.split(":")[1],
                    (data.comite_0_pais_2.split(":")[1] if data.comite_0_pais_2 else None),
                ],
            },
            {
                "nombre": data.comite_1,
                "opciones": [
                    data.comite_1_pais_0.split(":")[1],
                    data.comite_1_pais_1.split(":")[1],
                    (data.comite_1_pais_2.split(":")[1] if data.comite_1_pais_2 else None),
                ],
            },
            {
                "nombre": data.comite_2,
                "opciones": [
                    data.comite_2_pais_0.split(":")[1],
                    data.comite_2_pais_1.split(":")[1],
                    (data.comite_2_pais_2.split(":")[1] if data.comite_2_pais_2 else None),
                ],
            },
        ]
        data_obj = {
            "modalidad": "pareja" if es_codelegacion else "individual",
            "delegacion_oficial": delegacion_oficial,
            "participantes": participantes,
            "comites": comites,
        }
        now = datetime.now(timezone.utc)

        doc_ref = db_collection.document()
        doc_ref_id = doc_ref.id
        log_info(
            "delegaciones_created_document",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
        mime_type = mimetypes.guess_type(comprobante.filename)[0] or "application/octet-stream"
        try:
            extension = mimetypes.guess_extension(mime_type) or ".bin"
            blob_name = f"uploads/delegaciones/{'CODELEGACION' if es_codelegacion else 'DELEGACION'}_{data.nombre_0}_{data.apellido_0}_{doc_ref_id}{extension}"

            blob = comprobantes_bucket.blob(blob_name)
            comprobante.file.seek(0)
            try:
                blob.upload_from_file(comprobante.file, content_type=mime_type)
                log_info(
                    "upload_succeeded",
                    request_id=request_id,
                    submission_id=doc_ref_id,
                    submission_type=submission_type,
                    content_type=mime_type,
                    size_bytes=comprobante.size,
                    extension=extension,
                )

            except Exception:
                log_exception(
                    "upload_failed",
                    request_id=request_id,
                    submission_id=doc_ref_id,
                    submission_type=submission_type,
                    content_type=mime_type,
                    size_bytes=comprobante.size,
                    extension=extension,
                )
                release_claimed_idempotency_key(submission_type, idempotency_key, request_id)
                await comprobante.close()
                return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

            await comprobante.close()

            submission_payload = {
                "type": "delegacion",
                "status": "pending",
                "created_at": now,
                "updated_at": now,
                "file_path": blob_name,
                "request_id": request_id,
                "idempotency_key": idempotency_key,
                "data": data_obj,
            }
            outbox_payload = {
                "submission_id": doc_ref_id,
                "submission_type": submission_type,
                "request_id": request_id,
                "status": "pending",
                "created_at": now,
                "updated_at": now,
                "publish_started_at": None,
                "publish_lease_expires_at": None,
                "publisher_request_id": None,
                "last_error": None,
                "attempt_count": 0,
            }

            try:
                persist_submission_idempotency_and_outbox(
                    submission_ref=doc_ref,
                    submission_payload=submission_payload,
                    outbox_payload=outbox_payload,
                    submission_type=submission_type,
                    idempotency_key=idempotency_key,
                    request_id=request_id,
                )

            except LostIdempotencyClaimError:
                log_warning(
                    "idempotency_claim_lost_before_persist",
                    request_id=request_id,
                    submission_id=doc_ref_id,
                    submission_type=submission_type,
                )

                try:
                    blob.delete()
                except Exception:
                    log_exception(
                        "upload_cleanup_failed",
                        request_id=request_id,
                        submission_id=doc_ref_id,
                        submission_type=submission_type,
                        file_path=blob_name,
                    )

                return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

            except Exception:
                persist_outcome = inspect_persist_submission_outcome(
                    submission_ref=doc_ref,
                    submission_type=submission_type,
                    idempotency_key=idempotency_key,
                    request_id=request_id,
                )

                if persist_outcome == "committed":
                    log_warning(
                        "submission_commit_reconciled_after_exception",
                        request_id=request_id,
                        submission_id=doc_ref_id,
                        submission_type=submission_type,
                    )
                    return RedirectResponse(
                        get_confirmation_redirect(submission_type, frontend_origin),
                        status_code=status.HTTP_303_SEE_OTHER,
                    )

                if persist_outcome == "not_committed":
                    release_claimed_idempotency_key(submission_type, idempotency_key, request_id)
                    try:
                        blob.delete()
                    except Exception:
                        log_exception(
                            "upload_cleanup_failed",
                            request_id=request_id,
                            submission_id=doc_ref_id,
                            submission_type=submission_type,
                            file_path=blob_name,
                        )

                else:
                    log_warning(
                        "submission_commit_outcome_ambiguous",
                        request_id=request_id,
                        submission_id=doc_ref_id,
                        submission_type=submission_type,
                    )

                return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

            log_info(
                "submission_committed",
                request_id=request_id,
                submission_id=doc_ref_id,
                submission_type=submission_type,
            )
            return RedirectResponse(
                get_confirmation_redirect(submission_type, frontend_origin),
                status_code=status.HTTP_303_SEE_OTHER,
            )

        except Exception:
            await comprobante.close()
            raise

    except Exception:
        release_claimed_idempotency_key(submission_type, idempotency_key, request_id)

        try:
            await comprobante.close()
        except Exception:
            pass

        log_exception(
            "unexpected_post_claim_failure",
            request_id=request_id,
            submission_type=submission_type,
        )
        return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

    finally:
        stop_idempotency_claim_heartbeat(heartbeat_stop_event, heartbeat_thread)


# Accept, deduplicate, and persist a faculty submission
@router.post("/registro/faculty")
async def registrar_faculty(
    request: Request,
    data: FacultyFormData = Depends(),
    comprobante: UploadFile = File(...),
):
    request_id = getattr(request.state, "request_id", str(uuid4()))
    frontend_origin = get_frontend_origin(request)
    submission_type = "faculty"
    normalized_idempotency_key = validate_idempotency_key(request_id, data.idempotency_key)
    log_info(
        "faculty_start",
        request_id=request_id,
        numero_delegaciones=data.numero_delegaciones,
    )

    if comprobante.content_type is None or comprobante.size is None:
        raise_validation_error(request_id, "No se envió la imagen.")

    if not (comprobante.content_type.startswith("image/") or comprobante.content_type == "application/pdf") or comprobante.size > 5242880:
        raise_validation_error(
            request_id,
            "Imagen inválida.",
            content_type=comprobante.content_type,
            size=comprobante.size,
        )

    file_hash = await get_upload_file_hash(comprobante)

    if int(data.numero_delegaciones) < 4:
        raise_validation_error(
            request_id,
            "Número de delegaciones inválido.",
            numero_delegaciones=data.numero_delegaciones,
        )

    form = await request.form()
    delegaciones = parse_delegaciones_faculty(form, int(data.numero_delegaciones))

    if len(delegaciones) != int(data.numero_delegaciones):
        raise_validation_error(
            request_id,
            "Número de delegaciones no coincide.",
            esperado=int(data.numero_delegaciones),
            recibido=len(delegaciones),
        )

    payload_hash = get_payload_hash(
        data,
        submission_type,
        file_hash,
        {"delegaciones": delegaciones},
    )
    idempotency_key = normalized_idempotency_key or payload_hash
    idempotency_result = claim_idempotency_key(
        submission_type,
        idempotency_key,
        payload_hash,
        request_id,
    )

    if idempotency_result["result"] == "replay":
        replay_submission_id = idempotency_result.get("submission_id")
        replay_submission_status = get_submission_status(replay_submission_id) if replay_submission_id is not None else None

        if replay_submission_id is not None and replay_submission_status == "failed":
            if normalized_idempotency_key is None:
                if not reopen_failed_fallback_idempotency_key(
                    submission_type,
                    idempotency_key,
                    request_id,
                    replay_submission_id,
                ):
                    log_warning(
                        "failed_submission_fallback_retry_unavailable",
                        request_id=request_id,
                        submission_id=replay_submission_id,
                        submission_type=submission_type,
                    )
                    await comprobante.close()
                    return error_redirect(
                        submission_type,
                        frontend_origin,
                        rotate_idempotency_key=False,
                    )

                log_warning(
                    "failed_submission_fallback_retry_reclaimed",
                    request_id=request_id,
                    submission_id=replay_submission_id,
                    submission_type=submission_type,
                )
                idempotency_result = {"result": "claimed", "recovered": True}

            else:
                log_warning(
                    "failed_submission_replay_requires_new_attempt",
                    request_id=request_id,
                    submission_id=replay_submission_id,
                    submission_type=submission_type,
                )
                await comprobante.close()
                return error_redirect(
                    submission_type,
                    frontend_origin,
                    rotate_idempotency_key=True,
                )

        if idempotency_result["result"] == "replay":
            log_info(
                "idempotency_replayed",
                request_id=request_id,
                submission_type=submission_type,
                idempotency_status=idempotency_result.get("status"),
                submission_id=replay_submission_id,
            )
            await comprobante.close()
            return RedirectResponse(
                get_confirmation_redirect(submission_type, frontend_origin),
                status_code=status.HTTP_303_SEE_OTHER,
            )

    if idempotency_result["result"] == "conflict":
        log_warning(
            "idempotency_conflict",
            request_id=request_id,
            submission_type=submission_type,
            idempotency_status=idempotency_result.get("status"),
        )
        await comprobante.close()
        return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=True)

    if idempotency_result["result"] == "in_progress":
        log_warning(
            "idempotency_in_progress",
            request_id=request_id,
            submission_type=submission_type,
            idempotency_status=idempotency_result.get("status"),
        )
        await comprobante.close()
        return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

    log_info(
        ("idempotency_claimed" if not idempotency_result.get("recovered") else "idempotency_claim_recovered"),
        request_id=request_id,
        submission_type=submission_type,
    )
    heartbeat_stop_event, heartbeat_thread = start_idempotency_claim_heartbeat(submission_type, idempotency_key, request_id)
    try:
        data_obj = {
            "institucion": data.institucion_delegacion_oficial,
            "faculty": {
                "nombre": data.nombre_faculty,
                "apellido": data.apellido_faculty,
                "celular": data.celular_faculty,
                "correo": data.correo_faculty,
                "ciudad_estado": data.ciudad_estado_faculty,
                "pais": data.pais_faculty,
            },
            "numero_delegaciones": int(data.numero_delegaciones),
            "delegaciones": delegaciones,
        }
        now = datetime.now(timezone.utc)

        doc_ref = db_collection.document()
        doc_ref_id = doc_ref.id
        log_info(
            "faculty_created_document",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
        mime_type = mimetypes.guess_type(comprobante.filename)[0] or "application/octet-stream"
        extension = mimetypes.guess_extension(mime_type) or ".bin"
        blob_name = f"uploads/faculty/FACULTY_{data.institucion_delegacion_oficial}_{doc_ref_id}{extension}"

        blob = comprobantes_bucket.blob(blob_name)
        comprobante.file.seek(0)
        try:
            blob.upload_from_file(comprobante.file, content_type=mime_type)
            log_info(
                "upload_succeeded",
                request_id=request_id,
                submission_id=doc_ref_id,
                submission_type=submission_type,
                content_type=mime_type,
                size_bytes=comprobante.size,
                extension=extension,
            )

        except Exception:
            log_exception(
                "upload_failed",
                request_id=request_id,
                submission_id=doc_ref_id,
                submission_type=submission_type,
                content_type=mime_type,
                size_bytes=comprobante.size,
                extension=extension,
            )
            release_claimed_idempotency_key(submission_type, idempotency_key, request_id)
            await comprobante.close()
            return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

        await comprobante.close()

        submission_payload = {
            "type": "faculty",
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "file_path": blob_name,
            "request_id": request_id,
            "idempotency_key": idempotency_key,
            "data": data_obj,
        }
        outbox_payload = {
            "submission_id": doc_ref_id,
            "submission_type": submission_type,
            "request_id": request_id,
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "publish_started_at": None,
            "publish_lease_expires_at": None,
            "publisher_request_id": None,
            "last_error": None,
            "attempt_count": 0,
        }

        try:
            persist_submission_idempotency_and_outbox(
                submission_ref=doc_ref,
                submission_payload=submission_payload,
                outbox_payload=outbox_payload,
                submission_type=submission_type,
                idempotency_key=idempotency_key,
                request_id=request_id,

            )

        except LostIdempotencyClaimError:
            log_warning(
                "idempotency_claim_lost_before_persist",
                request_id=request_id,
                submission_id=doc_ref_id,
                submission_type=submission_type,

            )
            try:
                blob.delete()

            except Exception:
                log_exception(
                    "upload_cleanup_failed",
                    request_id=request_id,
                    submission_id=doc_ref_id,
                    submission_type=submission_type,
                    file_path=blob_name,
                )

            return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

        except Exception:
            persist_outcome = inspect_persist_submission_outcome(
                submission_ref=doc_ref,
                submission_type=submission_type,
                idempotency_key=idempotency_key,
                request_id=request_id,
            )

            if persist_outcome == "committed":
                log_warning(
                    "submission_commit_reconciled_after_exception",
                    request_id=request_id,
                    submission_id=doc_ref_id,
                    submission_type=submission_type,
                )
                return RedirectResponse(
                    get_confirmation_redirect(submission_type, frontend_origin),
                    status_code=status.HTTP_303_SEE_OTHER,
                )

            if persist_outcome == "not_committed":
                release_claimed_idempotency_key(submission_type, idempotency_key, request_id)
                try:
                    blob.delete()

                except Exception:
                    log_exception(
                        "upload_cleanup_failed",
                        request_id=request_id,
                        submission_id=doc_ref_id,
                        submission_type=submission_type,
                        file_path=blob_name,
                    )
            else:
                log_warning(
                    "submission_commit_outcome_ambiguous",
                    request_id=request_id,
                    submission_id=doc_ref_id,
                    submission_type=submission_type,
                )

            return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

        log_info(
            "submission_committed",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
        return RedirectResponse(
            get_confirmation_redirect(submission_type, frontend_origin),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    except Exception:
        release_claimed_idempotency_key(submission_type, idempotency_key, request_id)
        try:
            await comprobante.close()
        except Exception:
            pass

        log_exception(
            "unexpected_post_claim_failure",
            request_id=request_id,
            submission_type=submission_type,
        )
        return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

    finally:
        stop_idempotency_claim_heartbeat(heartbeat_stop_event, heartbeat_thread)


app.include_router(router)
