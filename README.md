# Payment Processing API

![CI/CD](https://github.com/MayhurJoker007/payment-processing-api/actions/workflows/ci.yml/badge.svg)

A payment service in the style of a card or EFT gateway: merchants submit payment requests, they're processed
asynchronously against an acquirer, and every step of the lifecycle is tracked and published as an event.

The point of the project is **reliability**: never charging twice, never losing a payment, and recovering by
itself when something in the middle falls over.

**Stack:** Python 3.12, FastAPI, SQLAlchemy 2 + Alembic, PostgreSQL, Redis Streams (message broker),
Prometheus metrics, JSON logs, Docker, Kubernetes / OpenShift manifests, GitHub Actions building to GHCR.

```
merchant ─POST /v1/payments─► API ──(one transaction)──► payment row + outbox row
                                                               │
                                     relay ◄───────────────────┘
                                       │ publishes
                                       ▼
                              Redis Stream "payments" ── consumer group ──► processor pods ──► acquirer
                                                                               │
                                       reaper: finds stuck payments ──► requeue
```

## Lifecycle

```
pending ─► processing ─► succeeded ─► partially_refunded ─► refunded
   │                └──► failed
   └─► cancelled
```

Illegal moves (refunding a failed payment, cancelling one that's already processing) return `409`. Each change
is stored as a `PaymentEvent` and returned in `GET /v1/payments/{id}`.

## Reliability, piece by piece

**Idempotency keys.** `POST /v1/payments` and refunds require an `Idempotency-Key`.
- Same key and same body: the original payment comes back with `200` and `Idempotent-Replayed: true`.
- Same key and a *different* body: `422`. Quietly returning the old payment for a new request would be a bug.
- Keys are scoped per merchant, and a unique constraint covers two identical requests arriving at once.

**Transactional outbox.** The payment row and its event are written in the same database transaction. A separate
relay publishes outbox rows to Redis Streams. There's never a payment without its event, and never an event for a
change that rolled back.

**At-least-once processing, made safe.** Messages can be delivered twice (for example the relay crashes after
publishing but before marking the row). The processor checks the payment's state first, so a duplicate is a no-op.
The mock acquirer is also idempotent on our payment id. In the tests, a payment that times out twice and succeeds
on the third attempt is charged exactly **once**.

**Retries and recovery.**
- An acquirer timeout leaves the message unacknowledged. Another worker claims it after `PAYMENT_CLAIM_IDLE_MS`
  (`XAUTOCLAIM`), which also covers a worker pod dying mid-payment.
- After `PAYMENT_MAX_ATTEMPTS` it fails with `acquirer_unavailable`.
- Declines (insufficient funds, stolen card) are final and not retried.
- The **reaper** republishes any payment stuck in `processing` for more than 5 minutes, in case its message was lost.
- **Optimistic locking** (a version column) stops two workers both finalising the same payment.

**Money is integers.** Amounts are in minor units (`15000` is R150.00), and refunds can't exceed what's left,
which a database check constraint enforces too.

**Merchant isolation.** API keys are compared in constant time. Another merchant's payment id returns `404`,
not `403`, so ids can't be probed.

## Observability

- `/healthz` (liveness) and `/readyz` (checks the database) for Kubernetes probes.
- `/metrics` for Prometheus: request count and latency by route, payments created (new or replayed), payments
  finished by status, acquirer latency and errors.
- JSON logs, one object per line, with `request_id` (taken from `X-Request-Id` or generated, and echoed back) and
  fields like `payment_id` and `attempt`, ready for CloudWatch, Loki or ELK.

## Deployment

- `docker compose up --build` starts Postgres, Redis (append-only), the API, the relay, 2 processors and the reaper.
- `k8s/` targets **EKS or OpenShift**: rolling updates with zero unavailable, a PodDisruptionBudget, a migration
  init container, probes, Prometheus scrape annotations, non-root containers with all capabilities dropped and a
  read-only root filesystem. There's no fixed UID, and the image is group-0 writable, as OpenShift expects.
- **CI/CD:** tests, then migrations up and down on a Postgres service, then the image is built. On `main` it's
  pushed to GHCR, tagged with the commit SHA and `latest`.

## Try it

```
docker compose up --build

curl -X POST localhost:8000/v1/payments \
  -H "X-Api-Key: test_key_demo_shop" -H "Idempotency-Key: order-10001" -H "Content-Type: application/json" \
  -d '{"amount": 15000, "currency": "ZAR", "payment_method": "tok_flaky", "reference": "INV-10001"}'
```

The test payment methods are `tok_visa_ok`, `tok_insufficient`, `tok_stolen`, `tok_flaky` (times out twice, then
approves) and `tok_down` (always times out). Interactive docs are at `/docs`.

## Tests

```
pip install -r requirements-dev.txt
pytest
```

The 13 tests use SQLite and fakeredis to run the real relay and processor code: the full lifecycle, idempotent
replays and mismatches, per-merchant keys, final declines, retried timeouts with a single charge, giving up after
max attempts, duplicate delivery, reaping a payment whose message was lost, partial and full refunds, cancel rules,
merchant isolation, outbox events, and the ops endpoints. The same flow was also run against PostgreSQL 16 with the
Alembic migration applied and rolled back.
