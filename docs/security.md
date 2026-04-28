# Security design

The SMMUN registration system is designed around public form ingestion, private file storage, asynchronous processing, and reliable recovery from partial failures.

## Ingestion controls

- The public API exposes only POST registration endpoints for submission intake.
- FastAPI and Pydantic validate form fields before persistence.
- Additional backend rules validate committee choices, participant counts, faculty delegation counts, official delegation fields, and age ranges.
- Uploaded comprobantes are limited to approved image/PDF extensions, MIME validation with a HEIC/HEIF mobile-client fallback, and a 5 MiB size cap.
- Validation failures are logged with sanitized context to avoid storing raw personal data in logs.

## Idempotency and replay handling

- The frontend sends an `idempotency_key` with each form submission.
- The API validates the key and stores it in a dedicated Firestore idempotency collection.
- The API computes a SHA-256 payload hash over normalized fields and uploaded file bytes.
- Same key plus same payload returns the canonical submission outcome.
- Same key plus different payload is treated as a conflict and the frontend is told to rotate the key.
- During rollout, submissions without an explicit key fall back to a deterministic payload-hash key.

This protects against accidental double-submits, browser retries, and replay of the same accepted payload.

## Durable acceptance boundary

The API success boundary is durable acceptance, not downstream completion.

When a submission is accepted, the API writes these records in one Firestore transaction:

- submission document
- committed idempotency record
- outbox row for asynchronous publishing

The API returns success only after those records are durable. Email and Google Sheets writes happen later in the worker.

## Reliable asynchronous delivery

The system uses a Firestore outbox to avoid losing accepted submissions between database writes and Pub/Sub publication.

- API creates outbox rows with `status = "pending"`.
- A Firestore create trigger publishes new rows to Pub/Sub.
- A scheduled sweep retries pending rows and stale publishing leases.
- Outbox rows move through `pending`, `publishing`, and `sent`.
- The active publisher lease is tracked so stale invocations do not overwrite newer attempts.

## Worker idempotency

Pub/Sub can deliver messages more than once, so the worker claims each submission transactionally.

- `pending` submissions can move to `processing`.
- `processing`, `completed`, and `failed` submissions are skipped.
- Successful processing marks the submission `completed`.
- Exceptions mark the submission `failed` and record an error message.

This prevents duplicate processing when Pub/Sub redelivers a message.

## File security

- Comprobantes are stored in Cloud Storage.
- The API validates upload size, MIME type, and filename extension before hashing and storing each file, allowing empty or generic HEIC/HEIF MIME values used by some mobile clients.
- The bucket uses uniform bucket-level access.
- Public access prevention is enforced.
- Files are not made public.
- The worker generates signed URLs for controlled access when sending downstream notifications.

## Service separation and IAM

Terraform defines separate service accounts for the API and worker-related functions. IAM is structured around a least-privilege model per component: each component receives only the categories of access it needs for its runtime responsibility.

API service account:

- writes uploaded files to the comprobantes bucket
- reads and writes Firestore submission, idempotency, and outbox data

Worker service account:

- reads uploaded files
- reads and writes Firestore submission state
- publishes Pub/Sub messages for outbox processing
- accesses the Resend API key from Secret Manager
- invokes the relevant Cloud Run function services

Current IAM is separated by component, but some roles are still project-scoped. A future hardening pass should review whether those roles can be narrowed without breaking deployment or runtime behavior.

## Secrets

- The Resend API key is stored in Google Secret Manager.
- The worker receives access through IAM rather than a plaintext repository value.
- The API does not need the Resend secret.

## Observability and incident tracing

- The API accepts or generates an `X-Request-ID`.
- Structured JSON logs include `request_id`, component name, event name, severity, and relevant sanitized context.
- Logs attach `logging.googleapis.com/trace` so related events can be correlated in Google Cloud Logging.
- The request ID is propagated into submission records, outbox rows, and Pub/Sub messages.
- The API emits `potential_abuse_detected` when one Cloud Run instance observes more than 10 registration POST attempts from the same hashed socket peer source in 60 seconds.
- Abuse tracking is bounded to 1024 sources per instance and evicts stale or least-recently-active sources.

## Current non-goals

- The API is public by design.
- The in-memory abuse signal is not a global rate limiter and may be noisy behind Cloud Run's public proxy path.
- The project does not currently include malware scanning for uploaded files.
- The project does not currently enforce Cloud Armor or reCAPTCHA.
- Worker downstream side effects are not retried automatically after a `failed` terminal state.
