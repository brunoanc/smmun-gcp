"""CloudEvent entrypoint and Pub/Sub message decoding for the worker."""

import functions_framework
import json
import base64
from uuid import uuid4
from runtime import log_info, log_warning, process_submission


# Decode a Pub/Sub event and dispatch it to the worker flow
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
