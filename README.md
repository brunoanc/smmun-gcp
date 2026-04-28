# SMMUN Registration Platform

![GCP](https://img.shields.io/badge/Cloud-GCP-blue)
![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688)
![Python](https://img.shields.io/badge/Language-Python-yellow)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC)

[🌐 smmun.com](https://smmun.com)

Cloud-native registration system for SMMUN, built around a static public frontend, a FastAPI ingestion service, and an asynchronous processing pipeline on Google Cloud Platform.

The platform accepts delegate and faculty registration forms, stores payment/comprobante uploads privately, records submissions in Firestore, and processes downstream side effects through Google Sheets and Resend without keeping the browser request open.

## Architecture

![Architecture Diagram](./docs/architecture.svg)

The submission flow is intentionally split into a fast acceptance path and a retryable background path:

1. Users submit registration forms through the static frontend hosted on Firebase Hosting.
2. The FastAPI service validates form data, uploads comprobantes to a private Cloud Storage bucket, and commits the submission, idempotency record, and outbox row in a single Firestore transaction.
3. The API returns a confirmation redirect once the submission is durably accepted.
4. An outbox publisher publishes accepted submissions to Pub/Sub using Firestore triggers and a scheduled recovery sweep.
5. A worker claims submissions with a Firestore lease, generates signed URLs, writes rows to Google Sheets, and sends confirmation emails through Resend.

Firestore tracks each submission through this lifecycle:

```text
pending -> processing -> completed / failed
```

Supporting documentation:

- [Data model and processing flow](./docs/data-model.md)
- [Security design](./docs/security.md)
- [Threat model](./docs/threat-model.md)

## Stack

Google Cloud:

- Cloud Run
- Cloud Storage
- Firestore
- Pub/Sub
- Cloud Run functions

External services:

- Google Sheets API
- Resend API

Frontend:

- Svelte
- Firebase Hosting

Infrastructure:

- Terraform
- GitHub Actions

## Reliability

The backend is designed so a registration can be accepted safely even when downstream services are unavailable or slow.

- API-level idempotency prevents duplicate submissions from browser retries, repeated clicks, and network retries.
- Firestore transactions define the acceptance boundary by committing the submission, idempotency record, and outbox row together.
- The Firestore outbox recovers from failures between database persistence and Pub/Sub publication.
- Worker leases prevent concurrent processing of the same submission.
- Worker checkpoints let retries skip completed side effects where possible.
- Structured logs propagate a `request_id` across API, Firestore, Pub/Sub, outbox, and worker events.

## Security

The system is designed for public form ingestion with private file handling and least-privilege service separation.

- Uploads are validated for size, MIME type, and extension before being stored.
- Comprobantes are written to a private Cloud Storage bucket with public access prevention.
- Signed URLs are generated only when downstream staff notifications need controlled file access.
- The Resend API key is stored in Secret Manager and exposed only to the worker service account.
- Service accounts are split by component so API, outbox, and worker permissions can be scoped independently.
- Suspicious high-frequency submission attempts emit structured warning signals without logging raw personal data.

See [Security Design](./docs/security.md) and [Threat Model](./docs/threat-model.md) for implementation details.

## Repository Layout

- `api/` - FastAPI service deployed on Cloud Run
- `outbox/` - Firestore outbox publisher and scheduled sweep handlers
- `worker/` - asynchronous submission processor
- `front/` - Svelte frontend
- `infra/` - Terraform configuration for Google Cloud resources
- `docs/` - architecture, data model, security, and threat model documentation

## Local Development

Each service keeps its dependencies close to its runtime directory.

```bash
pip install -r api/requirements.txt
pip install -r outbox/requirements.txt
pip install -r worker/requirements.txt
```

The frontend can be built from its own project directory:

```bash
cd front/smmun-svelte
npm ci
npm run build
```

Deployment configuration is managed through Terraform in `infra/`, with CI workflows under `.github/workflows/`.

## Project Context

SMMUN is a Model United Nations conference. This repository focuses on the public registration workflow and the operational pipeline behind it: reliable form intake, private document handling, durable submission state, and asynchronous integrations with the tools used by the organizing team.

## Author

Bruno Ancona - Software Engineering @ Universidad Anáhuac Mayab
