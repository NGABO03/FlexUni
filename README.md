# FlexUni Production v1 — Monetization + Deployment

This release extends the FlexUni production foundation with provider-neutral monetization and deployment infrastructure.

## Included
- Student, Student Plus and Business plan architecture
- Subscription records with provider/customer/subscription IDs
- Business accounts
- Payment event/webhook ledger
- Provider-neutral checkout endpoint (does not charge money by itself)
- PostgreSQL-ready `DATABASE_URL` handling
- Flask-Migrate dependency and migration bootstrap script
- Gunicorn production server configuration
- Dockerfile and Procfile
- `/healthz` deployment health check
- Secure production environment template
- Proxy-aware deployment configuration

## Run locally
```bash
python -m venv .venv
# activate it
pip install -r requirements.txt
flask --app app run
```

For local development, the app can create tables automatically. For production, set `AUTO_CREATE_DB=0` and run the migration bootstrap against PostgreSQL.

## Production deployment
1. Provision PostgreSQL.
2. Set a strong `SECRET_KEY`, `DATABASE_URL`, `COOKIE_SECURE=1`, and `AUTO_CREATE_DB=0`.
3. Run `./scripts_bootstrap.sh` once to initialize and apply migrations.
4. Start with Gunicorn (`gunicorn app:app`) or use the included Dockerfile.
5. Configure HTTPS at the platform/load balancer and configure your payment provider's signed webhook secret in the deployment secret manager.

## Payments
The `/billing/checkout` route intentionally creates a `pending` subscription only. It does **not** claim that a payment succeeded. A real provider adapter should create the hosted checkout session, verify webhook signatures, and update `Subscription.status` only from verified provider events.


## Phase 2 — Realtime
- WebSocket endpoint: `/ws` for authenticated users.
- Realtime message delivery and typing indicators.
- Realtime notification events and unread badge updates.
- HTTP polling remains as a resilient fallback for messages.
- The connection registry is process-local; for multiple Gunicorn workers/instances, add a shared Redis pub/sub adapter before horizontal scaling.
- Production Gunicorn uses the gevent worker class.

## Production hardening in this release
- CSRF protection remains enabled for state-changing browser requests.
- Payment webhooks are correctly exempt from browser CSRF and must pass HMAC signature verification.
- Secure response headers include CSP, Permissions-Policy, Referrer-Policy and conditional HSTS.
- Login restores the user's active university context.
- A pytest suite covers CSRF, registration/membership, signed webhooks and security headers.

Run checks locally after installing dependencies:
```bash
pytest -q
python -m py_compile app.py
```


## Production infrastructure added in the hardening build

- **PostgreSQL 18** is the production database target; PostgreSQL's current supported releases include 18 and 17. citeturn0search3
- **Redis** is used as the shared realtime WebSocket pub/sub bus when `REDIS_URL` is configured, allowing multiple web processes to fan out realtime events.
- **S3-compatible object storage** can replace local `static/uploads` by setting `MEDIA_STORAGE=s3` plus the `S3_*` variables. This is suitable for cloud object storage/CDN setups.
- **Live AI provider adapter** supports an OpenAI-compatible `/chat/completions` endpoint through `AI_BASE_URL`, `AI_API_KEY`, and `AI_MODEL`. The app keeps a safe fallback when these are not configured.
- **WebRTC configuration endpoint** `/api/rtc-config` exposes `RTC_ICE_SERVERS_JSON` to authenticated clients. Use STUN/TURN in production; the current UI now requests browser camera/microphone permission and loads the configured ICE servers.
- **Readiness endpoint** `/readyz` separates deployment readiness from `/healthz`.
- **Docker Compose** now provides a local production-like stack: web + PostgreSQL + Redis.

### Production environment

Copy `.env.production.example` into your deployment secret manager. Do not commit real credentials. For a cloud deployment, keep PostgreSQL and Redis managed where possible, use S3-compatible object storage for media, and put the Flask/Gunicorn service behind HTTPS. PostgreSQL provides ACID data integrity and supports high-concurrency production workloads. citeturn0search7turn0search9

### What still requires provider credentials

The code is now **integration-ready**, but no external account can be created from this source tree. Before public launch you still need to configure your chosen AI provider, payment provider, object-storage bucket, and TURN/media infrastructure. The app deliberately does not pretend those external services are live until their credentials are supplied.
