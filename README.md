# Project 2: Event-Driven Order System

A learning project, not a storefront. It builds a deliberately unsafe
database-plus-Kafka dual-write order system, reproduces its two
inconsistency windows on purpose, then evolves it — transactional
outbox, idempotent consumers, event choreography, compensation, and an
optional saga orchestrator — until the same failures are handled
correctly. See `PROJECT_2_SPEC.md` for the full contract this repository
implements, milestone by milestone.

## What's built so far (M00)

Workspace scaffolding only: the Cargo workspace, shared crates, four
service skeletons (health endpoints only, no business logic yet), and
local infrastructure. Business logic starts at M01. Track progress in
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

Then, in separate terminals, run any of the M00 service skeletons:

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

Both currently return `200 ok`. `/health/ready` will start checking real
dependency connectivity once a service has a database/broker pool to
check (M01+).

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

## Correlation IDs, idempotency, dual-write lab, outbox recovery

Not implemented yet — these land with M01 (orders API), M02 (dual-write
failure lab), and M03 (transactional outbox). This section will be
filled in as each milestone completes; see `docs/progress.md` for current
status and `PROJECT_2_SPEC.md` sections 11–14 for what's coming.

## Delivery semantics

At-least-once transport, effectively-once business effects — never a
claim of end-to-end exactly-once delivery. See spec section 5.

## Testing and chaos commands

```sh
make fmt              # cargo fmt --all
make lint              # cargo clippy --workspace --all-targets --all-features -- -D warnings
make test-unit          # cargo test --workspace --lib --bins
make test-integration   # not yet implemented (M03/M04)
make test-e2e            # not yet implemented (M06/M07)
make test                 # fmt + lint + test-unit (full required suite grows with milestones)
make demo-naive-failure    # not yet implemented (M02)
make chaos-smoke             # not yet implemented (M09)
make logs ORDER_ID=<uuid>     # tails docker compose logs filtered to one order
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
`FAILURE_INJECTION_ENABLED`, `DELIVERY_MODE`). Copy it to `.env` (done
automatically by `make setup`) and adjust as needed. Never commit `.env`.

## Known limitations

- M00 only: no order domain logic, no outbox/inbox tables, no Kafka
  producers/consumers, no persistence wiring. `/health/ready` does not
  yet check any dependency.
- No orchestrator exists; it is optional and only added after M10
  (core milestones) are complete, per spec section 24.

## Runbook

Operational runbook (`docs/runbook.md`) is added with M09 once there is
something to operate (retries, DLQ, compensation, chaos controls).
