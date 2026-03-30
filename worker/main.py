from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.discovery import build
from google.auth import default
from google.auth.transport.requests import Request as TransportRequest
from google.cloud import storage, firestore
from datetime import timedelta
import functions_framework
import logging
import os
import time
import json
import resend
import base64
from uuid import uuid4
import traceback

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
logger = logging.getLogger(__name__)
LOG_COMPONENT = "worker"


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


# HTML email templates
with (
    open("email/codelegacion.html", "r", encoding="utf-8") as co,
    open("email/delegacion.html", "r", encoding="utf-8") as dg,
    open("email/faculty.html", encoding="utf-8") as fac,
):
    html_emails = {
        "codelegacion": co.read(),
        "delegacion": dg.read(),
        "faculty": fac.read(),
    }


def comite_corto_a_largo(comite):
    match comite:
        case "SOCHUM":
            return "Tercera Comisión de la Asamblea General referente a lo Social, Cultural, Humanitario y de Derechos Humanos (SOCHUM)"
        case "ONU SIDA":
            return (
                "Programa Conjunto de las Naciones Unidas para el VIH-SIDA (ONU SIDA)"
            )
        case "ONU-Hábitat":
            return "Programa de las Naciones Unidas para los Asentamientos Humanos (ONU-Hábitat)"
        case "CCPCJ":
            return "Comisión de prevención del delito y Justicia Penal de las Naciones Unidas (CCPCJ)"
        case "UNRWA":
            return "Agencia de las Naciones Unidas para los Refugiados de Palestina en Oriente Próximo (UNRWA)"
        case "Cumbre":
            return "Cumbre del Futuro"
        case "NASA":
            return "Administración Nacional de Aeronáutica y del Espacio (NASA)"
        case "WWF":
            return "World Wildlife Fund for Nature (WWF)"
        case "Crisis":
            return "Crisis Futura"
        case "FIA":
            return "Federación Internacional del Automóvil (FIA)"
        case "FHCM":
            return "Federación de Alta Costura y Moda (FHCM)"
        case _:
            return comite


def cell(value):
    if value is None:
        return {}

    if isinstance(value, bool):
        return {"userEnteredValue": {"boolValue": value}}

    if isinstance(value, int):
        return {"userEnteredValue": {"numberValue": value}}

    if isinstance(value, float):
        return {"userEnteredValue": {"numberValue": value}}

    return {"userEnteredValue": {"stringValue": str(value)}}


def manejar_inscripcion(data: dict, request_id: str):
    inscripcion = data["data"]
    submission_type = "delegacion"
    log_info(
        "delegacion_processing_started",
        request_id=request_id,
        submission_type=submission_type,
    )

    # Get temporary signed URL for comprobante
    comprobante_path = data["file_path"]
    blob = comprobantes_bucket.blob(comprobante_path)
    url = blob.generate_signed_url(
        expiration=timedelta(days=7),
        method="GET",
        version="v4",
        service_account_email=google_credentials.service_account_email,
        access_token=google_credentials.token,
    )

    codelegacion = inscripcion["modalidad"] == "pareja"
    p1 = inscripcion["participantes"][0]
    p2 = (
        inscripcion["participantes"][1]
        if codelegacion and len(inscripcion["participantes"]) > 1
        else {}
    )

    created_at = data.get("created_at")
    fecha_str = created_at.strftime("%d/%m/%Y, %H:%M:%S") if created_at else ""

    # Add to inscripciones sheets
    row_values = [
        False,
        fecha_str,
        codelegacion,
        inscripcion.get("delegacion_oficial", {}).get("nombre"),
        inscripcion.get("delegacion_oficial", {}).get("responsable"),
        p1.get("nombre"),
        p1.get("apellido"),
        p1.get("edad"),
        p1.get("celular"),
        p1.get("correo"),
        p1.get("pais"),
        p1.get("ciudad_estado"),
        p1.get("escolaridad"),
        p1.get("escuela"),
        p1.get("contacto_emergencia"),
        p1.get("info_extra"),
        p2.get("nombre"),
        p2.get("apellido"),
        p2.get("edad"),
        p2.get("celular"),
        p2.get("correo"),
        p2.get("pais"),
        p2.get("ciudad_estado"),
        p2.get("escolaridad"),
        p2.get("escuela"),
        p2.get("contacto_emergencia"),
        p2.get("info_extra"),
        inscripcion["comites"][0]["nombre"],
        inscripcion["comites"][0]["opciones"][0],
        inscripcion["comites"][0]["opciones"][1],
        (
            inscripcion["comites"][0]["opciones"][2]
            if len(inscripcion["comites"][0]["opciones"]) > 2
            else None
        ),
        inscripcion["comites"][1]["nombre"],
        inscripcion["comites"][1]["opciones"][0],
        inscripcion["comites"][1]["opciones"][1],
        (
            inscripcion["comites"][1]["opciones"][2]
            if len(inscripcion["comites"][1]["opciones"]) > 2
            else None
        ),
        inscripcion["comites"][2]["nombre"],
        inscripcion["comites"][2]["opciones"][0],
        inscripcion["comites"][2]["opciones"][1],
        (
            inscripcion["comites"][2]["opciones"][2]
            if len(inscripcion["comites"][2]["opciones"]) > 2
            else None
        ),
        url,
    ]

    append_cells_request = {
        "requests": [
            {
                "appendCells": {
                    "tableId": os.environ["DELEGACIONES_TABLE_ID"],
                    "rows": [{"values": [cell(v) for v in row_values]}],
                    "fields": "*",
                    "sheetId": os.environ["DELEGACIONES_SHEET_ID"],
                }
            }
        ]
    }

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=os.environ["DELEGACIONES_SPREADSHEET_ID"],
        body=append_cells_request,
    ).execute()

    # Send email
    destinatarios = list(filter(None, [p1.get("correo"), p2.get("correo")]))

    comite_1_largo = comite_corto_a_largo(inscripcion["comites"][0]["nombre"])
    comite_2_largo = comite_corto_a_largo(inscripcion["comites"][1]["nombre"])
    comite_3_largo = comite_corto_a_largo(inscripcion["comites"][2]["nombre"])

    tipo_delegacion = "Codelegación" if codelegacion else "Delegación individual"
    do_texto = (
        inscripcion.get("delegacion_oficial", {}).get("nombre")
        if inscripcion.get("delegacion_oficial", {}).get("is_oficial")
        else "No"
    )
    responsable_do = (
        inscripcion.get("delegacion_oficial", {}).get("responsable")
        if inscripcion.get("delegacion_oficial", {}).get("is_oficial")
        else "No aplica"
    )

    if codelegacion:
        html = html_emails["codelegacion"].format(
            tipo_delegacion=tipo_delegacion,
            do_texto=do_texto,
            responsable_do=responsable_do,
            nombre=p1.get("nombre"),
            apellido=p1.get("apellido"),
            edad=p1.get("edad"),
            celular=p1.get("celular"),
            correo=p1.get("correo"),
            ciudad_estado=p1.get("ciudad_estado"),
            pais=p1.get("pais"),
            escolaridad=p1.get("escolaridad"),
            escuela=p1.get("escuela"),
            contacto_emergencia=p1.get("contacto_emergencia"),
            info_extra=p1.get("info_extra"),
            nombre_co=p2.get("nombre"),
            apellido_co=p2.get("apellido"),
            edad_co=p2.get("edad"),
            celular_co=p2.get("celular"),
            correo_co=p2.get("correo"),
            ciudad_estado_co=p2.get("ciudad_estado"),
            pais_co=p2.get("pais"),
            escolaridad_co=p2.get("escolaridad"),
            escuela_co=p2.get("escuela"),
            contacto_emergencia_co=p2.get("contacto_emergencia"),
            info_extra_co=p2.get("info_extra"),
            comite_1_largo=comite_1_largo,
            comite_1_opcion_1=inscripcion["comites"][0]["opciones"][0],
            comite_1_opcion_2=inscripcion["comites"][0]["opciones"][1],
            comite_1_opcion_3=inscripcion["comites"][0]["opciones"][2],
            comite_2_largo=comite_2_largo,
            comite_2_opcion_1=inscripcion["comites"][1]["opciones"][0],
            comite_2_opcion_2=inscripcion["comites"][1]["opciones"][1],
            comite_2_opcion_3=inscripcion["comites"][1]["opciones"][2],
            comite_3_largo=comite_3_largo,
            comite_3_opcion_1=inscripcion["comites"][2]["opciones"][0],
            comite_3_opcion_2=inscripcion["comites"][2]["opciones"][1],
            comite_3_opcion_3=inscripcion["comites"][2]["opciones"][2],
        )
    else:
        html = html_emails["delegacion"].format(
            tipo_delegacion=tipo_delegacion,
            do_texto=do_texto,
            responsable_do=responsable_do,
            nombre=p1.get("nombre"),
            apellido=p1.get("apellido"),
            edad=p1.get("edad"),
            celular=p1.get("celular"),
            correo=p1.get("correo"),
            ciudad_estado=p1.get("ciudad_estado"),
            pais=p1.get("pais"),
            escolaridad=p1.get("escolaridad"),
            escuela=p1.get("escuela"),
            contacto_emergencia=p1.get("contacto_emergencia"),
            info_extra=p1.get("info_extra"),
            comite_1_largo=comite_1_largo,
            comite_1_opcion_1=inscripcion["comites"][0]["opciones"][0],
            comite_1_opcion_2=inscripcion["comites"][0]["opciones"][1],
            comite_1_opcion_3=inscripcion["comites"][0]["opciones"][2],
            comite_2_largo=comite_2_largo,
            comite_2_opcion_1=inscripcion["comites"][1]["opciones"][0],
            comite_2_opcion_2=inscripcion["comites"][1]["opciones"][1],
            comite_2_opcion_3=inscripcion["comites"][1]["opciones"][2],
            comite_3_largo=comite_3_largo,
            comite_3_opcion_1=inscripcion["comites"][2]["opciones"][0],
            comite_3_opcion_2=inscripcion["comites"][2]["opciones"][1],
            comite_3_opcion_3=inscripcion["comites"][2]["opciones"][2],
        )

    resend.Emails.send(
        {
            "from": "Secretaría de Finanzas SMMUN <secretariadefinanzas@smmun.com>",
            "to": destinatarios,
            "subject": "¡Gracias! - SMMUN 2026: Una Nueva Historia",
            "html": html,
        }
    )
    log_info(
        "delegacion_processing_finished",
        request_id=request_id,
        submission_type=submission_type,
        recipients=len(destinatarios),
    )


def manejar_inscripcion_faculty(data: dict, request_id: str):
    inscripcion = data["data"]
    submission_type = "faculty"
    log_info(
        "faculty_processing_started",
        request_id=request_id,
        submission_type=submission_type,
    )

    # Get temporary signed URL for comprobante
    comprobante_path = data["file_path"]
    blob = comprobantes_bucket.blob(comprobante_path)
    url = blob.generate_signed_url(
        expiration=timedelta(days=7),
        method="GET",
        version="v4",
        service_account_email=google_credentials.service_account_email,
        access_token=google_credentials.token,
    )

    timestamp = int(time.time())

    # Add new page to Sheets
    title = f"{inscripcion['institucion']}_{timestamp}"
    body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"], body=body
    ).execute()

    created_at = data.get("created_at")
    fecha_str = created_at.strftime("%d/%m/%Y, %H:%M:%S") if created_at else ""

    # Add to inscripciones sheets
    row_values = [
        False,
        fecha_str,
        inscripcion["institucion"],
        inscripcion["numero_delegaciones"],
        inscripcion["faculty"]["nombre"],
        inscripcion["faculty"]["apellido"],
        inscripcion["faculty"]["celular"],
        inscripcion["faculty"]["correo"],
        inscripcion["faculty"]["pais"],
        inscripcion["faculty"]["ciudad_estado"],
        url,
    ]

    append_cells_request = {
        "requests": [
            {
                "appendCells": {
                    "tableId": os.environ["FACULTY_GENERAL_TABLE_ID"],
                    "rows": [{"values": [cell(v) for v in row_values]}],
                    "fields": "*",
                    "sheetId": os.environ["FACULTY_GENERAL_SHEET_ID"],
                }
            }
        ]
    }

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"], body=append_cells_request
    ).execute()

    body = {
        "values": [
            [
                inscripcion["institucion"],
            ],
            [
                "Nombre:",
                f"{inscripcion['faculty']['nombre']} {inscripcion['faculty']['apellido']}",
            ],
            ["Celular:", inscripcion["faculty"]["celular"]],
            ["Correo:", inscripcion["faculty"]["correo"]],
            [
                "Lugar de residencia:",
                f"{inscripcion['faculty']['ciudad_estado']}, {inscripcion['faculty']['pais']}",
            ],
            ["Número de delegaciones:", inscripcion["numero_delegaciones"]],
            ["Fecha de inscripción", fecha_str],
            ["Comprobante de pago:", url],
            [],
            [
                "Nombre",
                "Apellido",
                "Edad",
                "Celular",
                "Correo",
                "Lugar de residencia",
                "Escolaridad",
                "Escuela",
            ],
        ]
    }

    delegaciones = []

    for i in range(inscripcion["numero_delegaciones"]):
        body["values"].append(
            [
                inscripcion["delegaciones"][i].get("nombre"),
                inscripcion["delegaciones"][i].get("apellido"),
                inscripcion["delegaciones"][i].get("edad"),
                inscripcion["delegaciones"][i].get("celular"),
                inscripcion["delegaciones"][i].get("correo"),
                f"{inscripcion['delegaciones'][i].get('ciudad_estado')}, {inscripcion['delegaciones'][i].get('pais')}",
                inscripcion["delegaciones"][i].get("escolaridad"),
                inscripcion["delegaciones"][i].get("escuela"),
            ]
        )

        delegaciones_row = [
            inscripcion["institucion"],
            inscripcion["delegaciones"][i].get("nombre"),
            inscripcion["delegaciones"][i].get("apellido"),
            inscripcion["delegaciones"][i].get("edad"),
            inscripcion["delegaciones"][i].get("celular"),
            inscripcion["delegaciones"][i].get("correo"),
            f"{inscripcion['delegaciones'][i].get('ciudad_estado')}, {inscripcion['delegaciones'][i].get('pais')}",
            inscripcion["delegaciones"][i].get("escolaridad"),
            inscripcion["delegaciones"][i].get("escuela"),
        ]

        delegaciones.append({"values": [cell(v) for v in delegaciones_row]})

    sheets_service.spreadsheets().values().append(
        spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"],
        range=f"{title}!A:A",
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()

    append_cells_request = {
        "requests": [
            {
                "appendCells": {
                    "tableId": os.environ["FACULTY_DELEGACIONES_TABLE_ID"],
                    "rows": delegaciones,
                    "fields": "*",
                    "sheetId": os.environ["FACULTY_DELEGACIONES_SHEET_ID"],
                }
            }
        ]
    }

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"], body=append_cells_request
    ).execute()

    html = html_emails["faculty"].format(
        institucion_delegacion_oficial=inscripcion["institucion"],
        numero_delegaciones=inscripcion["numero_delegaciones"],
        nombre_faculty=inscripcion["faculty"]["nombre"],
        apellido_faculty=inscripcion["faculty"]["apellido"],
        celular_faculty=inscripcion["faculty"]["celular"],
        correo_faculty=inscripcion["faculty"]["correo"],
        ciudad_estado_faculty=inscripcion["faculty"]["ciudad_estado"],
        pais_faculty=inscripcion["faculty"]["pais"],
    )

    resend.Emails.send(
        {
            "from": "Secretaría de Finanzas SMMUN <secretariadefinanzas@smmun.com>",
            "to": [inscripcion["faculty"]["correo"]],
            "subject": "¡Gracias! - SMMUN 2026: Una Nueva Historia",
            "html": html,
        }
    )

    resend.Emails.send(
        {
            "from": "Secretaría de Finanzas SMMUN <secretariadefinanzas@smmun.com>",
            "to": "secretariadefinanzas@smmun.com",
            "subject": f"FACULTY: {inscripcion['faculty']['nombre']} {inscripcion['faculty']['apellido']}",
            "html": html,
        }
    )
    log_info(
        "faculty_processing_finished",
        request_id=request_id,
        submission_type=submission_type,
    )


def claim_submission(doc_ref):
    transaction = db_client.transaction()

    @firestore.transactional
    def _claim(transaction, doc_ref):
        doc = doc_ref.get(transaction=transaction)

        if not doc.exists:
            return False

        data = doc.to_dict()

        # Failed submissions are also skipped here because downstream side effects are not idempotent.
        if data.get("status") in ["processing", "completed", "failed"]:
            return False

        transaction.update(doc_ref, {"status": "processing"})
        return True

    return _claim(transaction, doc_ref)


def process_submission(
    submission_id: str, request_id: str, submission_type: str | None = None
):
    log_info(
        "submission_processing_started",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
    )

    google_credentials.refresh(TransportRequest())

    doc_ref = db_collection.document(submission_id)

    try:

        # Moving to processing is the worker's durable claim point.
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
            manejar_inscripcion(data, request_id)
        else:
            manejar_inscripcion_faculty(data, request_id)

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

    except Exception as e:
        log_exception(
            "submission_processing_failed",
            request_id=request_id,
            submission_id=submission_id,
            submission_type=submission_type,
        )

        doc_ref.update({"status": "failed", "error_message": str(e)})


@functions_framework.cloud_event
def pubsub_handler(cloud_event):
    encoded_data = cloud_event.data["message"]["data"]

    if not encoded_data:
        log_warning("pubsub_message_missing_data")
        return

    decoded = json.loads(base64.b64decode(encoded_data).decode("utf-8"))
    submission_id = decoded["submission_id"]
    request_id = decoded.get("request_id") or str(uuid4())
    submission_type = decoded.get("submission_type")

    log_info(
        "pubsub_message_received",
        request_id=request_id,
        submission_id=submission_id,
        submission_type=submission_type,
    )
    process_submission(submission_id, request_id, submission_type)
