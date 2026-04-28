# SMMUN website and registration system

![GCP](https://img.shields.io/badge/Cloud-GCP-blue)
![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688)
![Python](https://img.shields.io/badge/Language-Python-yellow)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC)

[🌐 smmun.com](https://smmun.com)

Cloud-native, event-driven backend for handling conference registrations on Google Cloud Platform.

---

## Architecture

![Architecture Diagram](./docs/architecture.svg)

[Data Model](./docs/data-model.md)

---

## Overview

1. Users submit a form via a static frontend (Firebase Hosting)
2. The API (Cloud Run, FastAPI):
   - validates input
   - absorbs duplicate submits using an idempotency key
   - uploads files to Cloud Storage
   - stores the submission, idempotency record, and outbox job atomically in Firestore
   - returns confirmation once the submission is durably accepted
3. An outbox publisher (Cloud Run functions) asynchronously publishes pending submission events to Pub/Sub
   - one trigger reacts immediately to newly created outbox rows
   - a scheduled sweep revisits pending/stale rows until they are sent
4. A worker (Cloud Functions / Cloud Run functions) processes submissions asynchronously:
   - generates signed URLs
   - writes data to Google Sheets
   - sends confirmation emails (Resend)
5. Firestore tracks submission state:
    ```
    pending → processing → completed / failed
    ```


---

## Stack

**GCP**
- Cloud Run
- Cloud Storage
- Firestore
- Pub/Sub
- Cloud Run functions

**External**
- Google Sheets API
- Resend API

---

## Key points

- Event-driven architecture (Pub/Sub)
- Asynchronous processing via worker
- API-level idempotency for submission ingestion
- Reliable asynchronous delivery via Firestore outbox
- Secure file access with signed URLs
- Structured logging with request tracing
- Infrastructure managed with Terraform

---

## Security Design

This system is designed around secure public form ingestion, data integrity, and resilient asynchronous processing.

Key mechanisms:

- **Idempotent API ingestion:** prevents duplicate submissions and mitigates replay and duplicate submission scenarios under concurrent conditions using client idempotency keys and SHA-256 payload hashing.
- **Transactional acceptance boundary:** commits the submission, idempotency record, and outbox row atomically in Firestore before returning confirmation.
- **Reliable outbox delivery:** recovers from partial failure between database writes and Pub/Sub publication using pending rows, publishing leases, and a scheduled sweep.
- **Worker-side checkpoints:** transactionally claims submissions and records Sheets/Resend progress so Pub/Sub retries skip completed side effects where possible.
- **Private file handling:** validates upload size, MIME type, and extension before storing comprobantes in a private Cloud Storage bucket with public access prevention.
- **Service separation:** structures IAM around separate service accounts to follow a least-privilege model per component and stores the Resend API key in Secret Manager.
- **Request tracing:** propagates `request_id` through API, Firestore, Pub/Sub, outbox, and worker logs for debugging and incident analysis.
- **Abuse detection signals:** logs a non-blocking `potential_abuse_detected` signal when one API instance observes high-frequency registration attempts from the same hashed source.

See [Security Design](./docs/security.md) and [Threat Model](./docs/threat-model.md) for details.

---

## Structure

- `/api` FastAPI service (Cloud Run)
- `/worker` Submission processor (Cloud Run functions)
- `/outbox` Outbox publisher and sweep handlers (Cloud Run functions)
- `/front` Svelte frontend
- `/infra` Terraform code
- `/docs` Architecture diagram

---

## Author

Bruno Ancona - Software Engineering @ Universidad Anáhuac Mayab
