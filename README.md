# Project 2: Event-Driven Order System

A learning project, not a storefront. It builds a deliberately unsafe
database-plus-Kafka dual-write order system, reproduces its two
inconsistency windows on purpose, then evolves it — transactional
outbox, idempotent consumers, event choreography, compensation, and an
optional saga orchestrator — until the same failures are handled
correctly. See `PROJECT_2_SPEC.md` for the full contract this repository
implements, milestone by milestone.

## What's built so far (M00-M04)

Workspace scaffolding (M00), the orders service's local-consistency core
(M01: idempotent order creation, versioned state machine, transition
history), the naive dual-write stage plus its failure lab (M02), the
transactional outbox that replaces it as the default (M03), and the
inventory service's idempotent reservation consumer (M04): `orders` now
inserts a business mutation and its outbox event(s) in one database
transaction — since M04, that includes an explicit
`inventory.reserve_inventory` command alongside `order_created` (see
`docs/adr/0007-orders-emits-reserve-inventory-command.md`) — and a
background publisher worker claims rows with `FOR UPDATE SKIP LOCKED`
leases and publishes them independently of the request path, retrying
with full-jitter backoff on failure. `inventory` consumes that command
through the idempotent-inbox protocol (spec section 14): dedupe by
`(consumer, event_id)`, verify a duplicate's payload hash, decide
apply/stale/gap by aggregate version, reserve stock all-or-nothing with
sorted-order row locking (never oversells, never partially reserves a
multi-SKU request), reply with `reservation_succeeded`/`reservation_failed`
through its own outbox, and dead-letter anything it can't process
(malformed envelope, unsupported schema, out-of-order gap) without
blocking the rest of the partition. The naive publish path from M02 stays
runnable behind `DELIVERY_MODE=naive` so its failure lab remains
reproducible; `DELIVERY_MODE=outbox` is the default. Payments and
fulfilment remain health-endpoint skeletons (M05+). Track progress in
[`docs/progress.md`](docs/progress.md).

## Architecture

```text
Client
  |
  v
Orders API ---- PostgreSQL (orders + transitions + outbox + inbox)
  | outbox publisher
  v
Redpanda topics
  |--------------------|--------------------|
  v                    v                    v
Inventory          Payments             Fulfilment
PostgreSQL         PostgreSQL            PostgreSQL
 + outbox/inbox     + outbox/inbox        + outbox/inbox
  |                    |                    |
  +--------------------+--------------------+
                       events
                         |
                         v
                     Orders projection/state machine
```

Optional final extension: a saga orchestrator (M11) consumes workflow
events and emits commands. It does not exist until choreography (M06/M07)
is complete.

| Service | Owns | Port |
|---|---|---|
| orders | order lifecycle, client idempotency, transition history | 8081 |
| inventory | stock and reservations | 8082 |
| payments | payment attempts and refunds | 8083 |
| fulfilment | shipment/fulfilment creation | 8084 |
| saga-orchestrator (optional, M11) | saga instance and step state only | — |

Each service owns its tables exclusively — separate databases, separate
migrations, separate SQLx pools, no cross-service joins (see
`migrations/README.md`).

## Prerequisites

- Rust via `rustup` (toolchain pinned in `rust-toolchain.toml`, currently
  1.94.0, with `rustfmt` and `clippy` components).
- Docker with Compose v2 (`docker compose version`).

## Quick start (five minutes)

```sh
make setup   # validates tools, copies .env.example -> .env, builds the workspace
make up      # starts PostgreSQL + Redpanda + Redpanda Console, waits for health
make topics  # explicitly creates the workflow + DLQ topics (auto-creation is disabled)
```

Then, in separate terminals, run any service:

```sh
cargo run -p orders       # GET http://localhost:8081/health/live
cargo run -p inventory    # GET http://localhost:8082/health/live
cargo run -p payments     # GET http://localhost:8083/health/live
cargo run -p fulfilment   # GET http://localhost:8084/health/live
```

```sh
curl http://localhost:8081/health/live
curl http://localhost:8081/health/ready
```

`orders` and `inventory` check a real Postgres connection (bounded 2s
timeout) for `/health/ready`; `payments` and `fulfilment` still return
`200 ok` unconditionally until their own persistence lands (M05+).

### Inventory dev endpoints

`inventory` exposes dev/test-only stock seed/read endpoints (spec
section 6):

```sh
curl -X PUT http://localhost:8082/_dev/stock/SKU-1 \
  -H "Content-Type: application/json" -d '{"available_qty": 50}'
curl http://localhost:8082/_dev/stock/SKU-1
```

### Orders API example

```sh
curl -i -X POST http://localhost:8081/v1/orders \
  -H "Idempotency-Key: demo-key-001" \
  -H "Content-Type: application/json" \
  -d '{"items":[{"sku":"SKU-1","quantity":2,"unit_price_minor":1250}],"currency":"USD"}'
# -> 202 Accepted, Location: /v1/orders/<id>, body is the order representation

curl http://localhost:8081/v1/orders/<id>
curl http://localhost:8081/v1/orders/<id>/transitions
```

Repeating the same `POST` with the same `Idempotency-Key` and the same
body replays the original `202` response (no second order is created).
The same key with a different body returns `409 IDEMPOTENCY_KEY_REUSED`.

Pass `X-Correlation-ID: <uuid>` to control the order's correlation ID; if
omitted (or unparseable), one is generated. It is persisted on the order
row, returned in the response body's `correlation_id` field, and echoed
back as an `X-Correlation-ID` response header — the same ID a client
would later grep for in `make logs ORDER_ID=<uuid>` once events exist to
carry it (M02+).

Redpanda Console: http://localhost:8090. Postgres is reachable at
`localhost:55432` (not the default `5432` — see `.env.example`) with the
credentials in `.env`.

## Ports

| Service | Port |
|---|---|
| orders | 8081 |
| inventory | 8082 |
| payments | 8083 |
| fulfilment | 8084 |
| PostgreSQL | 55432 (host) → 5432 (container) |
| Redpanda (Kafka API) | 19092 |
| Redpanda Console | 8090 |

## Idempotency, correlation IDs, dual-write lab, outbox recovery

Idempotency and correlation-ID handling are implemented for order
creation (M01, see the curl example above and `docs/evidence/m01.md`).

**Naive dual-write lab (M02):** with `DELIVERY_MODE=naive` (opt-in since
M03 — `outbox` is now the default), every accepted `POST /v1/orders` call
commits the order in Postgres and then publishes `orders.order_created`
directly to Kafka, with nothing tying the two together. Run

```sh
make demo-naive-failure
```

to see both resulting atomicity gaps reproduced live against your local
stack: (1) the DB commits but the event is never published, and (2) both
the DB commit and the publish succeed, but a naive client retry (after
seeing a failure it can't distinguish from gap 1) causes a real duplicate
event on the broker. The script builds and runs `orders` itself with
failure injection enabled, prints each violation, and tears the process
down on exit — no manual setup beyond `make up` first. Full explanation
of why retries cannot close either gap: [`docs/failure-lab.md`](docs/failure-lab.md).
Deterministic, automated versions of the same two demonstrations:
`services/orders/tests/dual_write_tests.rs`.

**Transactional outbox (M03):** with `DELIVERY_MODE=outbox` (the
default), order creation inserts the order row and its
`orders.order_created` outbox row in one database transaction (spec
section 13) — the event can never be lost the way the naive path loses
it in gap 1, because there is no window between "the business change
committed" and "the event is durably recorded" for a fault to land in.
A background publisher worker independently claims unpublished rows
with `FOR UPDATE SKIP LOCKED` and a lease, publishes them, and retries
with full-jitter backoff on failure; a worker that dies between a
successful publish and marking the row published causes the row to be
republished once its lease expires — a legitimate at-least-once
duplicate, not a lost event. Deterministic, automated demonstrations of
all six M03 acceptance gates (atomic rollback, exactly-one-outbox-row,
crash-then-duplicate-then-published, two publishers sharing a backlog
without double-publishing, broker-outage backlog growth and drain, and
the closed lost-event window): `services/orders/tests/outbox_tests.rs`.
Backlog/publish/lease-recovery counters: `GET /metrics` on the orders
service.

**Idempotent inventory consumer (M04):** `orders`' outbox now carries a
second event alongside `order_created` — an explicit
`inventory.reserve_inventory` command, in the same transaction — which
`inventory` consumes through the eight-step protocol in spec section 14:
validate the envelope, claim `(consumer, event_id)` in its own inbox
table (a duplicate delivery is a safe no-op, verified by comparing the
stored payload hash), classify the aggregate version as apply/stale/gap,
reserve stock all-or-nothing with SKU-sorted row locking (never oversells
under concurrency, never partially reserves a multi-SKU request), publish
`reservation_succeeded`/`reservation_failed` through its own transactional
outbox, and only then advance its local offset ledger — never before the
business transaction (or a DLQ publish, for a message it can't process)
has already committed. `rskafka` has no consumer-group/offset-commit
protocol, so "commit the offset" here means a row in `inventory`'s own
`consumer_offsets` table rather than a broker API call; see
`docs/adr/0006-inbox-consumer-offset-ledger.md`. Malformed envelopes,
unsupported schema versions, and unresolved version gaps are dead-lettered
to `inventory.commands.v1.dlq` without blocking other messages on the
partition (invariant I15). Deterministic, automated demonstrations of all
five M04 acceptance gates: `services/inventory/tests/consumer_tests.rs`.

This section grows as later milestones (payments/fulfilment consumers,
choreography, M05+) land; see `docs/progress.md` for current status.

## Delivery semantics

At-least-once transport, effectively-once business effects — never a
claim of end-to-end exactly-once delivery. See spec section 5.

## Testing and chaos commands

```sh
make fmt              # cargo fmt --all
make lint              # cargo clippy --workspace --all-targets --all-features -- -D warnings
make test-unit          # cargo test --workspace --lib --bins
make test-integration    # real Postgres/Redpanda integration tests (orders + inventory)
make test-e2e             # not yet implemented (M06/M07)
make test                  # fmt + lint + test-unit + test-integration
make demo-naive-failure     # runs both dual-write failure demonstrations live (M02)
make chaos-smoke              # not yet implemented (M09)
make logs ORDER_ID=<uuid>      # tails docker compose logs filtered to one order
```

## Troubleshooting

- **Port 5432 already in use**: another local Postgres is likely running.
  This project defaults to host port `55432` for that reason
  (`.env.example`); override `POSTGRES_PORT` if `55432` also collides.
- **Redpanda unhealthy / crash-looping**: check `docker compose logs
  redpanda`. This image's `rpk redpanda start` does not accept a `--set`
  flag; auto-topic-creation is disabled post-startup by `make up`
  instead (`rpk cluster config set auto_create_topics_enabled false`),
  not via a start flag.
- **`make topics` reports a topic already exists**: harmless — the
  target is safe to re-run.

## Reset warning

`make reset` runs `docker compose down -v`, permanently deleting the
Postgres and Redpanda volumes (all local data). It refuses to run
without `CONFIRM=yes`:

```sh
make reset CONFIRM=yes
```

`make down` stops containers without deleting data.

## Configuration

All configuration is environment-variable based; see `.env.example` for
the full list (Postgres/Redpanda connection info, per-service ports,
`ENVIRONMENT`, `FAILURE_INJECTION_ENABLED`/`FAILURE_INJECTION_TOKEN`,
`DELIVERY_MODE`). Copy it to `.env` (done automatically by `make setup`)
and adjust as needed. Never commit `.env`.

Failure injection (spec section 17) is off by default and refuses to
start if `FAILURE_INJECTION_ENABLED=true` while `ENVIRONMENT=production`.
When enabled, `orders` mounts `PUT /_test/faults/{name}` and
`DELETE /_test/faults`, both requiring a matching `X-Test-Token` header
(`FAILURE_INJECTION_TOKEN`). See `docs/adr/0004-fault-injector-placement-and-control.md`.

## Known limitations

- M00-M04 only: `orders` and `inventory` are wired end-to-end (order
  creation → `reserve_inventory` command → reservation →
  `reservation_succeeded`/`reservation_failed`), but nothing yet reacts to
  inventory's outcome events — orders does not update its own status from
  them until M06's choreography lands (spec section 12's transition graph
  is encoded but only `PENDING` is reachable end-to-end so far). Payments
  and fulfilment remain health-endpoint skeletons with no persistence
  wiring, so their `/health/ready` doesn't check a dependency yet.
- An out-of-order aggregate version at the inventory consumer goes
  straight to DLQ (`EXPECTED_VERSION_GAP`) rather than through the bounded
  retry/buffer window spec section 14 describes — that window is M08's
  deliverable by name; see `docs/adr/0008-m04-gap-policy-scope-boundary.md`.
- The inventory consumer is not yet safe to run as multiple concurrent
  replicas against the same topic/partition (no consumer-group-style
  partition assignment) — see `docs/adr/0006-inbox-consumer-offset-ledger.md`'s
  consequences section.
- The naive publish path (still runnable behind `DELIVERY_MODE=naive`)
  republishes on every accepted create call, including idempotent
  replays — a deliberate, documented anti-pattern
  (`docs/adr/0003-naive-publish-on-every-replay.md`), not a bug; the
  outbox mode fixes this by only inserting an outbox row when a new
  order is genuinely created.
- The outbox publisher's retry backoff uses an unseeded RNG for M03
  (full-jitter per spec section 15); deterministic/seeded retry timing
  for tests is deferred to M05's full retry-taxonomy milestone.
- `POST /v1/orders/{id}/cancel` does not exist yet — it is optional per
  spec section 10 and deferred until the choreographed workflow (M06+)
  gives it something meaningful to cancel.
- No orchestrator exists; it is optional and only added after M10
  (core milestones) are complete, per spec section 24.

## Runbook

Operational runbook (`docs/runbook.md`) is added with M09 once there is
something to operate (retries, DLQ, compensation, chaos controls).
