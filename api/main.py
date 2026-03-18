from typing import Annotated, Optional, cast
from fastapi import FastAPI, APIRouter, Form, File, UploadFile, Depends, status, Request
from fastapi.responses import RedirectResponse
from pydantic import EmailStr
from dataclasses import dataclass
import logging
import unicodedata
from starlette.datastructures import FormData
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
from google.cloud import storage, firestore, pubsub_v1
from google.auth import default
from time import perf_counter
from uuid import uuid4
import traceback
import os
import json
import mimetypes


# URL de la página estática
URL_BASE = "https://smmun.com"

# Nombres provenientes de GCP
COMPROBANTES_BUCKET_NAME = os.environ["COMPROBANTES_BUCKET_NAME"]
FIRESTORE_COLLECTION_NAME = os.environ["FIRESTORE_COLLECTION_NAME"]
_, PROJECT_ID = default()
PUB_SUB_TOPIC_NAME = os.environ["PUB_SUB_TOPIC_NAME"]

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(message)s"
)
logger = logging.getLogger(__name__)
REQUEST_ID_HEADER = "X-Request-ID"
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
}


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


def _to_json_log(event: str, severity: str, **fields) -> str:
    request_id = fields.get("request_id")
    payload = {"severity": severity, "event": event, "component": LOG_COMPONENT, **fields}
    if request_id:
        payload["logging.googleapis.com/trace"] = f"projects/{PROJECT_ID}/traces/{request_id}"
    return json.dumps(payload, ensure_ascii=False, default=str)


def log_info(event: str, **fields):
    logger.info(_to_json_log(event, "INFO", **fields))


def log_warning(event: str, **fields):
    logger.warning(_to_json_log(event, "WARNING", **fields))


def log_exception(event: str, **fields):
    logger.error(_to_json_log(event, "ERROR", error=traceback.format_exc(), **fields))

# Lista de comités y tipos no permitidos en codelegación
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

TIPOS_SOLO_INDIVIDUAL = {"pilotos", "disenadores_emergentes", "astronautas", "representantes_nasa"}
COMITES_CON_TIPOS = {"fia", "fhcm", "nasa", "cumbre_futuro"}

# Cargar delegaciones para validaciones cruzadas
with open("delegaciones.json", "r", encoding="utf-8") as delegaciones_json:
    delegaciones_data = json.load(delegaciones_json)


# Clase para recibir y validar el forms de delegaciones
@dataclass
class DelegacionFormData:
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
    institucion_delegacion_oficial: str = Form(max_length=150)
    nombre_faculty: str = Form(max_length=150)
    apellido_faculty: str = Form(max_length=150)
    celular_faculty: str = Form(max_length=150)
    correo_faculty: EmailStr = Form(max_length=150)
    ciudad_estado_faculty: str = Form(max_length=150)
    pais_faculty: str = Form(max_length=150)
    numero_delegaciones: str = Form(max_length=2)


# Cloud Storage bucket
storage_client = storage.Client()
comprobantes_bucket = storage_client.bucket(COMPROBANTES_BUCKET_NAME)

# Firestore DB
db_client = firestore.Client()
db_collection = db_client.collection(FIRESTORE_COLLECTION_NAME)

# Pub/sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUB_SUB_TOPIC_NAME)

# Inicializar app y router
router = APIRouter()
app = FastAPI(docs_url=None, redoc_url=None)


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


# Mostrar página de error en vez de error en JSON
@app.exception_handler(StarletteHTTPException)
@app.exception_handler(RequestValidationError)
async def http_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    log_warning(
        "request_redirected_to_error",
        request_id=request_id,
        path=request.url.path,
        exception_type=type(exc).__name__,
    )
    return RedirectResponse(f"{URL_BASE}/registro/error/", status_code=status.HTTP_303_SEE_OTHER)


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://localhost:8080",
        "https://smmun.com",
        "https://www.smmun.com",
        "https://smmun0githubio-production.up.railway.app",
        "https://smmun0.github.io",
        "https://github.io"
    ],
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["*"],
)


def normalizar_comite(siglas: str) -> str:
    texto = unicodedata.normalize("NFD", siglas)
    texto = "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")
    texto = texto.lower().replace(" ", "_").replace("-", "_")
    if texto == "cumbre":
        return "cumbre_futuro"
    return texto


def obtener_tipo_delegacion(comite_siglas: str, delegacion_nombre: str) -> Optional[str]:
    clave = normalizar_comite(comite_siglas)
    data = delegaciones_data.get(clave)
    if not isinstance(data, dict):
        return None

    for tipo, lista in data.items():
        if any(item.get("nombre") == delegacion_nombre for item in lista):
            return tipo
    return None


def parse_delegacion(valor: str) -> tuple[str, str]:
    if ":" not in valor:
        raise ValueError("Delegación inválida.")
    comite_valor, delegacion = valor.split(":", 1)
    return comite_valor, delegacion


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

        # Skip completely empty rows (safety)
        if not nombre and not apellido:
            continue

        delegaciones.append({
            "nombre": nombre,
            "apellido": apellido,
            "edad": int(edad) if edad else None,
            "celular": celular,
            "correo": correo,
            "pais": pais,
            "ciudad_estado": ciudad_estado,
            "escolaridad": escolaridad,
            "escuela": escuela
        })

    return delegaciones

# Endpoint para el forms
@router.post("/registro/delegaciones")
async def registrar(request: Request, data: DelegacionFormData = Depends(), comprobante: UploadFile = File(...)):
    request_id = getattr(request.state, "request_id", str(uuid4()))
    submission_type = "delegacion"
    log_info("delegaciones_start", request_id=request_id, modalidad=data.modalidad)

    # Validar archivo
    if comprobante.content_type is None or comprobante.size is None:
        raise_validation_error(request_id, "No se envió la imagen.")

    if not (comprobante.content_type.startswith("image/") or comprobante.content_type == "application/pdf") or comprobante.size > 5242880:
        raise_validation_error(request_id, "Imagen inválida.", content_type=comprobante.content_type, size=comprobante.size)

    # Validar comités
    if data.modalidad == "pareja" and (data.comite_0 in ["Cumbre", "Crisis"] or data.comite_1 in ["Cumbre", "Crisis"] or data.comite_2 in ["Cumbre", "Crisis"]):
        raise_validation_error(request_id, "Opción inválida de comité para codelegación.", comites=[data.comite_0, data.comite_1, data.comite_2])

    comites = [data.comite_0, data.comite_1, data.comite_2]
    if len(comites) != len(set(comites)):
        raise_validation_error(request_id, "Opciones de comités repetidas.", comites=comites)

    if data.delegacion_oficial == "si" and (not data.nombre_delegacion_oficial or not data.responsable_delegacion_oficial):
        raise_validation_error(request_id, "Faltan datos de delegación oficial.")

    # Validar países
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
    
    # Validar edades
    if not 11 <= int(data.edad_0) <= 26 or (es_codelegacion and (data.edad_1 is None or not 11 <= int(data.edad_1) <= 26)):
        raise_validation_error(request_id, "Edad inválida.", edad_0=data.edad_0, edad_1=data.edad_1, modalidad=data.modalidad)

    # Validar datos de codelegación obligatorios
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

    # Validar tipos no permitidos en codelegación
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
                raise_validation_error(request_id, "Delegación inválida.", comite=comite_siglas, valor=valor)
            _, delegacion_nombre = parse_delegacion(valor)
            tipo = obtener_tipo_delegacion(comite_siglas, delegacion_nombre)
            if tipo in TIPOS_SOLO_INDIVIDUAL:
                raise_validation_error(request_id, "Delegación no disponible para codelegación.", comite=comite_siglas, delegacion=delegacion_nombre, tipo=tipo)

    # Crear documento para Firestore
    delegacion_oficial = {
        "is_oficial": data.delegacion_oficial == "si",
        "nombre": data.nombre_delegacion_oficial or "No aplica",
        "responsable": data.responsable_delegacion_oficial or "No aplica",
    }

    participantes = [
        {
            "nombre": data.nombre_0,
            "apellido": data.apellido_0,
            "edad": int(data.edad_0),
            "celular": data.celular_0,
            "correo": data.correo_0,
            "pais": data.pais_0,
            "ciudad_estado": data.ciudad_estado_0,
            "escolaridad": data.escolaridad_0,
            "escuela": data.escuela_0 or "No aplica",
            "contacto_emergencia": f"{data.nombre_contacto_0} ({data.relacion_contacto_0}): {data.celular_contacto_0}",
            "info_extra": data.info_extra_0
        }
    ]

    if es_codelegacion:
        participantes.append({
            "nombre": data.nombre_1,
            "apellido": data.apellido_1,
            "edad": int(cast(str, data.edad_1)),
            "celular": data.celular_1,
            "correo": data.correo_1,
            "pais": data.pais_1,
            "ciudad_estado": data.ciudad_estado_1,
            "escolaridad": data.escolaridad_1,
            "escuela": (data.escuela_1 or "No aplica"),
            "contacto_emergencia": f"{data.nombre_contacto_1} ({data.relacion_contacto_1}): {data.celular_contacto_1}",
            "info_extra": data.info_extra_1 if es_codelegacion and data.info_extra_1 else None
        })

    comites = [
        {
            "nombre": data.comite_0,
            "opciones": [
                data.comite_0_pais_0.split(":")[1],
                data.comite_0_pais_1.split(":")[1],
                data.comite_0_pais_2.split(":")[1] if data.comite_0_pais_2 else None
            ]
        },
        {
            "nombre": data.comite_1,
            "opciones": [
                data.comite_1_pais_0.split(":")[1],
                data.comite_1_pais_1.split(":")[1],
                data.comite_1_pais_2.split(":")[1] if data.comite_1_pais_2 else None
            ]
        },
        {
            "nombre": data.comite_2,
            "opciones": [
                data.comite_2_pais_0.split(":")[1],
                data.comite_2_pais_1.split(":")[1],
                data.comite_2_pais_2.split(":")[1] if data.comite_2_pais_2 else None
            ]
        }
    ]

    data_obj = {
        "modalidad": "pareja" if es_codelegacion else "individual",
        "delegacion_oficial": delegacion_oficial,
        "participantes": participantes,
        "comites": comites
    }

    now = datetime.now(timezone.utc)

    # Crear documento en base de datos
    doc_ref = db_collection.document()
    doc_ref_id = doc_ref.id
    log_info(
        "delegaciones_created_document",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
    )

    # Subir comprobante a Cloud Storage
    mime_type = mimetypes.guess_type(comprobante.filename)[0] or "application/octet-stream"
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
        raise

    await comprobante.close()

    doc_ref.set({
        "type": "delegacion",
        "status": "pending",

        "created_at": now,
        "updated_at": now,

        "file_path": blob_name,

        "data": data_obj
    })
    log_info(
        "firestore_write_succeeded",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
    )

    # Publicar evento
    try:
        publisher.publish(
            topic_path,
            json.dumps(
                {
                    "submission_id": doc_ref_id,
                    "request_id": request_id,
                    "submission_type": submission_type,
                }
            ).encode("utf-8")
        )
        log_info(
            "delegaciones_published_event",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
    except Exception:
        log_exception(
            "delegaciones_publish_failed",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
        raise ValueError("Error procesando la inscripción.")

    log_info(
        "delegaciones_completed",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
    )
    log_info(
        "delegaciones_terminal_success",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
        final_status="completed",
    )
    # Redirigir a página de confirmación
    return RedirectResponse(f"{URL_BASE}/registro/confirmacion/", status_code=status.HTTP_303_SEE_OTHER)

@router.post("/registro/faculty")
async def registrar_faculty(request: Request, data: FacultyFormData = Depends(), comprobante: UploadFile = File(...)):
    request_id = getattr(request.state, "request_id", str(uuid4()))
    submission_type = "faculty"
    log_info("faculty_start", request_id=request_id, numero_delegaciones=data.numero_delegaciones)

    # Validar archivo
    if comprobante.content_type is None or comprobante.size is None:
        raise_validation_error(request_id, "No se envió la imagen.")

    if not (comprobante.content_type.startswith("image/") or comprobante.content_type == "application/pdf") or comprobante.size > 5242880:
        raise_validation_error(request_id, "Imagen inválida.", content_type=comprobante.content_type, size=comprobante.size)

    if int(data.numero_delegaciones) < 4:
        raise_validation_error(request_id, "Número de delegaciones inválido.", numero_delegaciones=data.numero_delegaciones)

    # Obtener datos de delegaciones
    form = await request.form()
    delegaciones = parse_delegaciones_faculty(form, int(data.numero_delegaciones))

    if len(delegaciones) != int(data.numero_delegaciones):
        raise_validation_error(
            request_id,
            "Número de delegaciones no coincide.",
            esperado=int(data.numero_delegaciones),
            recibido=len(delegaciones),
        )

    # Crear documento para Firestore
    data_obj = {
        "institucion": data.institucion_delegacion_oficial,
        "faculty": {
            "nombre": data.nombre_faculty,
            "apellido": data.apellido_faculty,
            "celular": data.celular_faculty,
            "correo": data.correo_faculty,
            "ciudad_estado": data.ciudad_estado_faculty,
            "pais": data.pais_faculty
        },

        "numero_delegaciones": int(data.numero_delegaciones),
        "delegaciones": delegaciones
    }

    now = datetime.now(timezone.utc)

    # Crear documento en base de datos
    doc_ref = db_collection.document()
    doc_ref_id = doc_ref.id
    log_info(
        "faculty_created_document",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
    )

    # Subir comprobante a Cloud Storage
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
        raise

    await comprobante.close()

    doc_ref.set({
        "type": "faculty",
        "status": "pending",

        "created_at": now,
        "updated_at": now,

        "file_path": blob_name,

        "data": data_obj
    })
    log_info(
        "firestore_write_succeeded",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
    )

    # Publicar evento
    try:
        publisher.publish(
            topic_path,
            json.dumps(
                {
                    "submission_id": doc_ref_id,
                    "request_id": request_id,
                    "submission_type": submission_type,
                }
            ).encode("utf-8")
        )
        log_info(
            "faculty_published_event",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
    except Exception:
        log_exception(
            "faculty_publish_failed",
            request_id=request_id,
            submission_id=doc_ref_id,
            submission_type=submission_type,
        )
        raise ValueError("Error procesando la inscripción.")

    log_info(
        "faculty_completed",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
    )
    log_info(
        "faculty_terminal_success",
        request_id=request_id,
        submission_id=doc_ref_id,
        submission_type=submission_type,
        final_status="completed",
    )
    # Redirigir a página de confirmación
    return RedirectResponse(f"{URL_BASE}/registro/confirmacion/", status_code=status.HTTP_303_SEE_OTHER)


# Usar el router
app.include_router(router)
