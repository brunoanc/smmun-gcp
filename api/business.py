"""Business logic for the API: form models, validation rules, payload hashing, and route handlers."""

from dataclasses import dataclass, fields as dataclass_fields
from datetime import datetime, timezone
from pathlib import Path
from fastapi import Depends, File, Form, Request, UploadFile, status
from fastapi.responses import RedirectResponse
from pydantic import EmailStr
from typing import Optional, cast
import json
import unicodedata
import hashlib
from uuid import uuid4
from runtime import (
    IDEMPOTENCY_KEY_FIELD,
    LostIdempotencyClaimError,
    claim_idempotency_key,
    comprobantes_bucket,
    db_collection,
    error_redirect,
    get_confirmation_redirect,
    get_frontend_origin,
    get_submission_status,
    get_upload_file_hash,
    inspect_persist_submission_outcome,
    log_exception,
    log_info,
    log_warning,
    persist_submission_idempotency_and_outbox,
    raise_validation_error,
    release_claimed_idempotency_key,
    reopen_failed_fallback_idempotency_key,
    router,
    start_idempotency_claim_heartbeat,
    stop_idempotency_claim_heartbeat,
    validate_idempotency_key,
)

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

# Comprobante validation
MAX_COMPROBANTE_SIZE_BYTES = 5 * 1024 * 1024
ALLOWED_COMPROBANTE_MIME_TYPES_BY_EXTENSION = {
    ".pdf": {"application/pdf"},
    ".png": {"image/png"},
    ".jpg": {"image/jpeg", "image/jpg", "image/pjpeg"},
    ".jpeg": {"image/jpeg", "image/jpg", "image/pjpeg"},
    ".webp": {"image/webp"},
    ".heic": {"", "application/octet-stream", "image/heic", "image/heif", "image/heic-sequence"},
    ".heif": {"", "application/octet-stream", "image/heif", "image/heic", "image/heif-sequence"},
}


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
    escolaridad_0: str = Form(pattern=r"^(Secundaria|Preparatoria|Universidad|Egresado|No estudio)$", max_length=150)
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
    escolaridad_1: Optional[str] = Form(None, pattern=r"^(|Secundaria|Preparatoria|Universidad|Egresado|No estudio)$", max_length=150)
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

        # Skip completely empty rows
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
        "file": {"hash": file_hash},
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


# Build the delegacion data payload stored in Firestore
def build_delegacion_data(data: DelegacionFormData, edad_0: int, edad_1: int | None) -> dict:
    es_codelegacion = data.modalidad == "pareja"
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
                "escuela": data.escuela_1 or "No aplica",
                "contacto_emergencia": f"{data.nombre_contacto_1} ({data.relacion_contacto_1}): {data.celular_contacto_1}",
                "info_extra": data.info_extra_1 if data.info_extra_1 else None,
            }
        )

    comites = [
        {
            "nombre": data.comite_0,
            "opciones": [
                data.comite_0_pais_0.split(":")[1],
                data.comite_0_pais_1.split(":")[1],
                data.comite_0_pais_2.split(":")[1] if data.comite_0_pais_2 else None,
            ],
        },
        {
            "nombre": data.comite_1,
            "opciones": [
                data.comite_1_pais_0.split(":")[1],
                data.comite_1_pais_1.split(":")[1],
                data.comite_1_pais_2.split(":")[1] if data.comite_1_pais_2 else None,
            ],
        },
        {
            "nombre": data.comite_2,
            "opciones": [
                data.comite_2_pais_0.split(":")[1],
                data.comite_2_pais_1.split(":")[1],
                data.comite_2_pais_2.split(":")[1] if data.comite_2_pais_2 else None,
            ],
        },
    ]

    return {
        "modalidad": "pareja" if es_codelegacion else "individual",
        "delegacion_oficial": delegacion_oficial,
        "participantes": participantes,
        "comites": comites,
    }


# Build the faculty data payload stored in Firestore
def build_faculty_data(data: FacultyFormData, delegaciones: list[dict]) -> dict:
    return {
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


# Validate uploaded comprobante size, MIME type, and filename extension
def validate_comprobante_upload(request_id: str, comprobante: UploadFile) -> tuple[str, str]:
    if comprobante.size is None:
        raise_validation_error(request_id, "No se envió la imagen.")

    content_type = (comprobante.content_type or "").lower().split(";", 1)[0].strip()
    extension = Path(comprobante.filename or "").suffix.lower()
    allowed_content_types = ALLOWED_COMPROBANTE_MIME_TYPES_BY_EXTENSION.get(extension)

    if allowed_content_types is None or comprobante.size > MAX_COMPROBANTE_SIZE_BYTES:
        raise_validation_error(
            request_id,
            "Imagen inválida.",
            content_type=content_type,
            extension=extension,
            size=comprobante.size,
        )

    if content_type not in allowed_content_types:
        raise_validation_error(
            request_id,
            "Imagen inválida.",
            content_type=content_type,
            extension=extension,
            size=comprobante.size,
        )

    return content_type, extension


# Upload a comprobante file to Cloud Storage
def upload_comprobante(
    *,
    comprobante: UploadFile,
    blob_name: str,
    content_type: str,
    extension: str,
    request_id: str,
    submission_id: str,
    submission_type: str,
) -> tuple[str, object]:
    final_blob_name = f"{blob_name}{extension}"
    blob = comprobantes_bucket.blob(final_blob_name)
    comprobante.file.seek(0)

    try:
        blob.upload_from_file(comprobante.file, content_type=content_type)
        log_info(
            "upload_succeeded",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            content_type=content_type,
            size_bytes=comprobante.size,
            extension=extension,
        )
    except Exception:
        log_exception(
            "upload_failed",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
            content_type=content_type,
            size_bytes=comprobante.size,
            extension=extension,
        )
        raise

    return final_blob_name, blob


# Persist a successfully claimed submission and handle ambiguous commit outcomes
def persist_submission(
    *,
    request_id: str,
    frontend_origin: str,
    submission_type: str,
    doc_ref,
    doc_ref_id: str,
    blob,
    blob_name: str,
    idempotency_key: str,
    submission_payload: dict,
    outbox_payload: dict,
):
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


# Handle idempotency replay, conflict, and in-progress outcomes
async def handle_idempotency_result(
    *,
    request_id: str,
    frontend_origin: str,
    submission_type: str,
    normalized_idempotency_key: str | None,
    idempotency_key: str,
    idempotency_result: dict,
    comprobante: UploadFile,
):
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
                    return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=False)

                log_warning(
                    "failed_submission_fallback_retry_reclaimed",
                    request_id=request_id,
                    submission_id=replay_submission_id,
                    submission_type=submission_type,
                )
                return {"result": "claimed", "recovered": True}

            log_warning(
                "failed_submission_replay_requires_new_attempt",
                request_id=request_id,
                submission_id=replay_submission_id,
                submission_type=submission_type,
            )
            await comprobante.close()
            return error_redirect(submission_type, frontend_origin, rotate_idempotency_key=True)

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

    return idempotency_result


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

    comprobante_content_type, comprobante_extension = validate_comprobante_upload(request_id, comprobante)
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
        raise_validation_error(request_id, "Opciones de delegación repetidas.", comite=data.comite_0, opciones=paises_0)

    paises_1 = [data.comite_1_pais_0, data.comite_1_pais_1, data.comite_1_pais_2]
    if len(paises_1) != len(set(paises_1)):
        raise_validation_error(request_id, "Opciones de delegación repetidas.", comite=data.comite_1, opciones=paises_1)

    paises_2 = [data.comite_2_pais_0, data.comite_2_pais_1, data.comite_2_pais_2]
    if len(paises_2) != len(set(paises_2)):
        raise_validation_error(request_id, "Opciones de delegación repetidas.", comite=data.comite_2, opciones=paises_2)

    es_codelegacion = data.modalidad == "pareja"
    edad_0 = parse_age_value(request_id, data.edad_0, field_name="edad_0", modalidad=data.modalidad)
    edad_1 = parse_age_value(request_id, cast(str | None, data.edad_1), field_name="edad_1", modalidad=data.modalidad) if es_codelegacion else None

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
                raise_validation_error(request_id, "Delegación inválida.", comite=comite_siglas, valor=valor)

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

    payload_hash = get_payload_hash(data, submission_type, file_hash)
    idempotency_key = normalized_idempotency_key or payload_hash
    idempotency_result = claim_idempotency_key(submission_type, idempotency_key, payload_hash, request_id)
    idempotency_handled = await handle_idempotency_result(
        request_id=request_id,
        frontend_origin=frontend_origin,
        submission_type=submission_type,
        normalized_idempotency_key=normalized_idempotency_key,
        idempotency_key=idempotency_key,
        idempotency_result=idempotency_result,
        comprobante=comprobante,
    )

    if isinstance(idempotency_handled, RedirectResponse):
        return idempotency_handled

    idempotency_result = idempotency_handled
    log_info(
        "idempotency_claimed" if not idempotency_result.get("recovered") else "idempotency_claim_recovered",
        request_id=request_id,
        submission_type=submission_type,
    )
    heartbeat_stop_event, heartbeat_thread = start_idempotency_claim_heartbeat(submission_type, idempotency_key, request_id)

    try:
        data_obj = build_delegacion_data(data, edad_0, edad_1)
        now = datetime.now(timezone.utc)

        doc_ref = db_collection.document()
        doc_ref_id = doc_ref.id
        log_info(
            "delegaciones_created_document",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )

        try:
            blob_name, blob = upload_comprobante(
                comprobante=comprobante,
                blob_name=f"uploads/delegaciones/{'CODELEGACION' if es_codelegacion else 'DELEGACION'}_{data.nombre_0}_{data.apellido_0}_{doc_ref_id}",
                content_type=comprobante_content_type,
                extension=comprobante_extension,
                request_id=request_id,
                submission_id=doc_ref_id,
                submission_type=submission_type,
            )
        except Exception:
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

        return persist_submission(
            request_id=request_id,
            frontend_origin=frontend_origin,
            submission_type=submission_type,
            doc_ref=doc_ref,
            doc_ref_id=doc_ref_id,
            blob=blob,
            blob_name=blob_name,
            idempotency_key=idempotency_key,
            submission_payload=submission_payload,
            outbox_payload=outbox_payload,
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

    comprobante_content_type, comprobante_extension = validate_comprobante_upload(request_id, comprobante)
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

    payload_hash = get_payload_hash(data, submission_type, file_hash, {"delegaciones": delegaciones})
    idempotency_key = normalized_idempotency_key or payload_hash
    idempotency_result = claim_idempotency_key(submission_type, idempotency_key, payload_hash, request_id)
    idempotency_handled = await handle_idempotency_result(
        request_id=request_id,
        frontend_origin=frontend_origin,
        submission_type=submission_type,
        normalized_idempotency_key=normalized_idempotency_key,
        idempotency_key=idempotency_key,
        idempotency_result=idempotency_result,
        comprobante=comprobante,
    )

    if isinstance(idempotency_handled, RedirectResponse):
        return idempotency_handled

    idempotency_result = idempotency_handled
    log_info(
        "idempotency_claimed" if not idempotency_result.get("recovered") else "idempotency_claim_recovered",
        request_id=request_id,
        submission_type=submission_type,
    )
    heartbeat_stop_event, heartbeat_thread = start_idempotency_claim_heartbeat(submission_type, idempotency_key, request_id)

    try:
        data_obj = build_faculty_data(data, delegaciones)
        now = datetime.now(timezone.utc)

        doc_ref = db_collection.document()
        doc_ref_id = doc_ref.id
        log_info(
            "faculty_created_document",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )

        try:
            blob_name, blob = upload_comprobante(
                comprobante=comprobante,
                blob_name=f"uploads/faculty/FACULTY_{data.institucion_delegacion_oficial}_{doc_ref_id}",
                content_type=comprobante_content_type,
                extension=comprobante_extension,
                request_id=request_id,
                submission_id=doc_ref_id,
                submission_type=submission_type,
            )
        except Exception:
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

        return persist_submission(
            request_id=request_id,
            frontend_origin=frontend_origin,
            submission_type=submission_type,
            doc_ref=doc_ref,
            doc_ref_id=doc_ref_id,
            blob=blob,
            blob_name=blob_name,
            idempotency_key=idempotency_key,
            submission_payload=submission_payload,
            outbox_payload=outbox_payload,
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
