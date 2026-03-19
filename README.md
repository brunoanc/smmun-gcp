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
   - uploads files to Cloud Storage
   - stores metadata in Firestore
   - publishes an event to Pub/Sub
3. A worker (Cloud Functions / Cloud Run functions) processes submissions asynchronously:
   - generates signed URLs
   - writes data to Google Sheets
   - sends confirmation emails (Resend)
4. Firestore tracks submission state:
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
- Idempotent processing using Firestore state
- Secure file access with signed URLs
- Structured logging with request tracing
- Infrastructure managed with Terraform

---

## Structure

- `/api` FastAPI service (Cloud Run)
- `/worker` Event processor (Cloud Run functions)
- `/front` Svelte frontend
- `/infra` Terraform code
- `/docs` Architecture diagram

---

## Author

Bruno Ancona - Software Engineering @ Universidad Anáhuac Mayab
