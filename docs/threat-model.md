# Threat Model

This document describes the main abuse and reliability threats considered for the SMMUN registration pipeline.

The system accepts public registration forms, stores uploaded comprobantes, persists submissions in Firestore, publishes asynchronous work through Pub/Sub, and sends downstream side effects through Google Sheets and Resend.

## Assets

- Registration data submitted by delegates and faculty advisors
- Uploaded payment/comprobante files stored in Cloud Storage
- Submission state in Firestore
- Pub/Sub messages used to trigger asynchronous processing
- Resend API key stored in Secret Manager
- Google Sheets rows generated from accepted submissions

## Trust Boundaries

- Browser to public FastAPI API on Cloud Run
- API to Firestore and Cloud Storage
- Firestore outbox to Pub/Sub publisher function
- Pub/Sub to worker function
- Worker to Google Sheets, Resend, and Cloud Storage signed URL generation

## Threats and Mitigations

### Duplicate or replayed submissions

Vector: repeated HTTP POSTs, browser retries, network retries, or users submitting the same form multiple times.

Mitigations:

- Frontend sends an `idempotency_key` per submit attempt.
- API validates the key format and stores an idempotency record in Firestore.
- API computes a SHA-256 payload hash over normalized form fields and uploaded file bytes.
- Same key plus same payload replays the canonical submission result instead of creating a second submission.
- Same key plus different payload is rejected and instructs the browser to rotate the key.
- Firestore transactions commit the idempotency record, submission, and outbox row together.

Operational consideration:

- Idempotency protects duplicate acceptance, not volumetric denial of service. High-volume traffic is best handled with Cloud Armor or another edge control.

### Malformed or invalid input

Vector: direct API calls bypassing browser-side validation, malformed form fields, invalid committee selections, or unexpected faculty delegation counts.

Mitigations:

- FastAPI and Pydantic validate field presence, format, and maximum length.
- Backend enforces business rules such as age ranges, committee uniqueness, codelegation restrictions, official delegation fields, and faculty delegation count consistency.
- Validation failures are logged with sanitized context rather than raw submitted values.
- User-facing failures redirect to the frontend error page.

Operational consideration:

- Validation is application-specific and is reviewed whenever registration rules change.

### File upload abuse

Vector: oversized files, unsupported file types, or attempts to expose uploaded files publicly.

Mitigations:

- API accepts only approved image/PDF filename extensions.
- API validates MIME type against the filename extension, with a HEIC/HEIF fallback for empty or generic mobile-client MIME values.
- API rejects files larger than 5 MiB.
- Uploaded files are stored in a Cloud Storage bucket with uniform bucket-level access enabled.
- Public access prevention is enforced on the bucket.
- Worker generates signed URLs for controlled file access instead of exposing objects publicly.

Operational consideration:

- The system validates extension, MIME type, and size. Content sniffing or asynchronous malware scanning can be added as an operational control before broader staff distribution or higher-volume intake.

### Worker re-execution and Pub/Sub duplicate delivery

Vector: Pub/Sub at-least-once delivery, function retries, or duplicate messages for the same submission.

Mitigations:

- Worker claims a submission with a Firestore transaction.
- Active `processing` submissions raise a retryable error so Pub/Sub/Eventarc keeps redelivering instead of acknowledging the message too early.
- `completed`, `failed`, or missing submissions are skipped with a normal acknowledgement.
- Stale `processing` submissions can be reclaimed after the worker lease expires.
- The worker processes side effects only after the claim succeeds.
- Sheets and Resend side effects are recorded in Firestore checkpoints.
- Completed checkpoints are skipped on retry, so a retry resumes at the first incomplete side effect.
- Terminal status is persisted as `completed` only after all required checkpoints complete, or `failed` after an exception.

Operational consideration:

- Downstream side effects such as email and Google Sheets writes are not fully reversible. Resend receives deterministic idempotency keys, but Google Sheets uses Firestore checkpoint-only protection. If a worker crashes after a Sheets API success and before recording `sheets.completed`, a stale-lease retry can write to Sheets again and may require manual reconciliation. Failed submissions are not retried automatically because repeated downstream side effects may not be idempotent.

### Partial failure between database commit and message publication

Vector: API accepts a submission but crashes before publishing the Pub/Sub message.

Mitigations:

- API writes an outbox row atomically with the submission and idempotency commit.
- A Firestore create trigger publishes new outbox rows quickly.
- A scheduled sweep revisits pending rows and stale publishing leases.
- Outbox publication uses a lease owner field so stale invocations do not overwrite newer claims.

Operational consideration:

- Outbox recovery depends on the scheduled sweep and Firestore/Eventarc availability.

### Redirect and origin abuse

Vector: forged `Origin` or `Referer` headers attempting to redirect users to an untrusted site.

Mitigations:

- API redirects only to explicitly allowed frontend origins.
- Unknown origins fall back to `https://smmun.com`.
- CORS allows POST requests only from configured frontend origins.

Operational consideration:

- CORS is not an authentication boundary. Direct HTTP clients can still call public registration endpoints.

### Secret exposure

Vector: accidental disclosure of third-party API keys or embedding secrets into application code.

Mitigations:

- Resend API key is stored in Google Secret Manager.
- Terraform grants secret access to the worker service account.
- The API service does not receive the Resend secret.

Operational consideration:

- Operational access to Google Cloud resources must still be controlled outside this repository.

### High-frequency registration attempts

Vector: repeated registration POST requests from the same source.

Mitigations:

- API emits a structured `potential_abuse_detected` warning when an instance observes more than 10 registration attempts from the same hashed socket peer source in 60 seconds.
- The source identity is hashed before logging.
- Per-instance tracking is bounded and evicts stale or least-recently-active sources to avoid unbounded memory growth.
- Detection is intentionally non-blocking so legitimate users are not rejected by an in-memory heuristic.

Operational consideration:

- This signal is instance-local and best effort. It is not a distributed rate limiter and may be noisy behind Cloud Run's public proxy path. Enforcement belongs at the edge through Cloud Armor, reCAPTCHA, or another trusted control.
