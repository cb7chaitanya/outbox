# Progress

## Milestone checklist

- [x] M00 — Repository contract and skeleton
- [x] M01 — Orders API and local consistency
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

M01 complete. Next action: M02 — naive dual write and failure lab.

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
- Idempotency-key race safety: `INSERT ... ON CONFLICT (idempotency_key)
  DO NOTHING` rather than check-then-insert, relying on Postgres
  serializing concurrent inserts on the same unique key. See
  `docs/adr/0002-idempotency-key-race-safety.md`.
- `orders` is a lib+bin crate (`src/lib.rs` + `src/main.rs`), not
  bin-only — integration tests in `tests/` link the compiled library
  instead of re-including source files with `#[path]`, which avoids a
  false "dead code" state where the lint sees two independent, mostly-
  unused copies of the same module.
- Request-body idempotency comparison hashes a canonical form (items
  sorted by SKU, currency) with SHA-256, so item reordering doesn't count
  as "a different request" but a genuine content change does.
- `correlation_id` is a real column on `orders` (added in a second,
  additive migration after the initial schema was already applied and
  committed, per the forward-only migration rule) even though spec
  section 9's table listing doesn't show it — section 16 requires
  persisting it, and it needs a home before M02+ events can carry it.

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

## Commands run and results (M01)

All commands below were actually run in this repository state on
2026-08-19, against the M00 infrastructure (still running). Full
transcript excerpts, including the four M01 acceptance-gate proofs, are
in `docs/evidence/m01.md`.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `make test-unit` | all crates pass; orders lib: 10/10 |
| `make test-integration` | orders repository tests: 7/7; orders HTTP tests: 7/7 |
| `sqlx migrate run` (orders, both migrations) | applied cleanly against the running Postgres |
| `cargo run -p orders` + curl (create/get/transitions/404/ready) | all responses match spec section 10 exactly (see evidence) |

Total orders-service tests: 24 passed, 0 failed.

## Next action

Start M02: naive direct-to-Kafka publish after DB commit, event
contracts, the two deterministic fault points
(`orders.after_db_commit_before_publish`,
`orders.after_publish_before_response`), `scripts/demo-dual-write-
failure.sh`, and `docs/failure-lab.md` explaining why retries alone
cannot close the two atomicity gaps. Do not add outbox tables yet (spec
section 11 / M03).
