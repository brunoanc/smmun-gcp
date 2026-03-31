"""Business logic for the worker: Sheets writes, email content, and per-type processing."""

from datetime import timedelta
import os
import time
import resend
from runtime import (
    comprobantes_bucket,
    google_credentials,
    log_info,
    sheets_service,
)

# Email templates
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


# Expand short committee names for emails
def comite_corto_a_largo(comite):
    match comite:
        case "SOCHUM":
            return "Tercera Comisión de la Asamblea General referente a lo Social, Cultural, Humanitario y de Derechos Humanos (SOCHUM)"
        case "ONU SIDA":
            return "Programa Conjunto de las Naciones Unidas para el VIH-SIDA (ONU SIDA)"
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


# Convert Python values to Sheets API cell payloads
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


# Get a temporary signed URL for the uploaded comprobante
def build_signed_comprobante_url(comprobante_path: str) -> str:
    blob = comprobantes_bucket.blob(comprobante_path)
    return blob.generate_signed_url(
        expiration=timedelta(days=7),
        method="GET",
        version="v4",
        service_account_email=google_credentials.service_account_email,
        access_token=google_credentials.token,
    )


# Process one delegacion submission end to end
def process_delegacion_submission(data: dict, request_id: str):
    inscripcion = data["data"]
    submission_type = "delegacion"
    log_info(
        "delegacion_processing_started",
        request_id=request_id,
        submission_type=submission_type,
    )

    url = build_signed_comprobante_url(data["file_path"])

    codelegacion = inscripcion["modalidad"] == "pareja"
    p1 = inscripcion["participantes"][0]
    p2 = inscripcion["participantes"][1] if codelegacion and len(inscripcion["participantes"]) > 1 else {}
    created_at = data.get("created_at")
    fecha_str = created_at.strftime("%d/%m/%Y, %H:%M:%S") if created_at else ""

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
        (inscripcion["comites"][0]["opciones"][2] if len(inscripcion["comites"][0]["opciones"]) > 2 else None),
        inscripcion["comites"][1]["nombre"],
        inscripcion["comites"][1]["opciones"][0],
        inscripcion["comites"][1]["opciones"][1],
        (inscripcion["comites"][1]["opciones"][2] if len(inscripcion["comites"][1]["opciones"]) > 2 else None),
        inscripcion["comites"][2]["nombre"],
        inscripcion["comites"][2]["opciones"][0],
        inscripcion["comites"][2]["opciones"][1],
        (inscripcion["comites"][2]["opciones"][2] if len(inscripcion["comites"][2]["opciones"]) > 2 else None),
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

    destinatarios = list(filter(None, [p1.get("correo"), p2.get("correo")]))

    comite_1_largo = comite_corto_a_largo(inscripcion["comites"][0]["nombre"])
    comite_2_largo = comite_corto_a_largo(inscripcion["comites"][1]["nombre"])
    comite_3_largo = comite_corto_a_largo(inscripcion["comites"][2]["nombre"])

    tipo_delegacion = "Codelegación" if codelegacion else "Delegación individual"
    do_texto = inscripcion.get("delegacion_oficial", {}).get("nombre") if inscripcion.get("delegacion_oficial", {}).get("is_oficial") else "No"
    responsable_do = (
        inscripcion.get("delegacion_oficial", {}).get("responsable") if inscripcion.get("delegacion_oficial", {}).get("is_oficial") else "No aplica"
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


# Process one faculty submission end to end
def process_faculty_submission(data: dict, request_id: str):
    inscripcion = data["data"]
    submission_type = "faculty"
    log_info(
        "faculty_processing_started",
        request_id=request_id,
        submission_type=submission_type,
    )

    url = build_signed_comprobante_url(data["file_path"])
    timestamp = int(time.time())

    title = f"{inscripcion['institucion']}_{timestamp}"
    body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}

    sheets_service.spreadsheets().batchUpdate(spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"], body=body).execute()
    created_at = data.get("created_at")
    fecha_str = created_at.strftime("%d/%m/%Y, %H:%M:%S") if created_at else ""

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
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"], body=append_cells_request).execute()

    body = {
        "values": [
            [inscripcion["institucion"]],
            ["Nombre:", f"{inscripcion['faculty']['nombre']} {inscripcion['faculty']['apellido']}"],
            ["Celular:", inscripcion["faculty"]["celular"]],
            ["Correo:", inscripcion["faculty"]["correo"]],
            ["Lugar de residencia:", f"{inscripcion['faculty']['ciudad_estado']}, {inscripcion['faculty']['pais']}"],
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

    sheets_service.spreadsheets().batchUpdate(spreadsheetId=os.environ["FACULTY_SPREADSHEET_ID"], body=append_cells_request).execute()

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
