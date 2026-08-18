# Progress

## Milestone checklist

- [x] M00 — Repository contract and skeleton
- [ ] M01 — Orders API and local consistency
- [ ] M02 — Naive dual write and failure lab
- [ ] M03 — Transactional outbox
- [ ] M04 — Inventory consumer and idempotent inbox
- [ ] M05 — Payments with retry taxonomy
- [ ] M06 — Choreographed workflow and compensation
- [ ] M07 — Fulfilment and complete compensation matrix
- [ ] M08 — Ordering, replay, and concurrency hardening
- [ ] M09 — Observability, chaos, and operations
- [ ] M10 — Final acceptance, README, and learning write-up
- [ ] M11 — Optional saga orchestrator (only after M10)

## Current milestone

M00 complete. Next action: M01 — orders migrations, domain state machine,
create/get/transitions API, request validation, client idempotency,
optimistic versioning, structured errors, and tests (no broker publish yet
except a stub/port).

## Decisions

- Kafka/Redpanda client: `rskafka` (pure Rust) instead of `rdkafka`
  (`cmake`/`pkg-config` unavailable in this environment). See
  `docs/adr/0001-tech-stack-choices.md`.
- Config loading: `figment` with `ENV_` prefix per service
  (`ORDERS_*`, `INVENTORY_*`, `PAYMENTS_*`, `FULFILMENT_*`).
- Local Postgres host port defaults to `55432`, not `5432` — another
  project on this dev machine already binds `5432`. Documented in
  `.env.example`; the container's internal port is still the standard
  `5432`.
- `rpk redpanda start` in image `v24.2.7` has no `--set` flag (confirmed
  via `rpk redpanda start --help` inside the container). Auto-topic-
  creation is instead disabled post-startup with
  `rpk cluster config set auto_create_topics_enabled false`, run as a
  step in `make up` after the healthcheck passes.
- Edition 2024, toolchain pinned to the installed stable 1.94.0
  (`rust-toolchain.toml`).

## Commands run and results (M00)

All commands below were actually run in this repository state on
2026-08-19. Full transcript excerpts are in `docs/evidence/m00.md`.

| Command | Result |
|---|---|
| `cargo build --workspace` | success, 0 errors |
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `cargo test --workspace --lib --bins` | 9 tests passed, 0 failed |
| `docker compose down -v` then `make setup` | clean checkout simulation, succeeded |
| `make up` | postgres + redpanda healthy, `auto_create_topics_enabled=false` set |
| `make topics` | 14/14 topics created (7 workflow + 7 `.dlq`) |
| `cargo run -p {orders,inventory,payments,fulfilment}` + curl | all 4 services: `/health/live` 200, `/health/ready` 200 |
| `test -d services/saga-orchestrator` | absent, as required |

Infrastructure was left running (`docker compose up -d` state) after M00
verification since the next milestone (M01) needs Postgres immediately.

## Next action

Start M01: orders service migrations (own database, `services/orders/migrations/`),
`orders`/`order_items`/`order_transitions` tables per spec section 9,
domain state machine, `POST /v1/orders` with `Idempotency-Key` handling,
`GET /v1/orders/{id}`, `GET /v1/orders/{id}/transitions`, optimistic
versioning, structured `application/problem+json` errors, and the M01
acceptance tests (concurrent idempotent requests, reused-key conflict,
overflow/invalid item validation, illegal/stale transitions).
