# Progress

## Milestone checklist

- [x] M00 — Repository contract and skeleton
- [x] M01 — Orders API and local consistency
- [x] M02 — Naive dual write and failure lab
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

M02 complete. Next action: M03 — transactional outbox.

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
- The naive publish path republishes `orders.order_created` on every
  accepted `POST /v1/orders` call, including idempotent replays, rather
  than skipping replays — deliberately, since coupling the publish
  decision to the idempotency layer would already be an outbox-shaped
  fix. See `docs/adr/0003-naive-publish-on-every-replay.md`.
- `FaultInjector` (spec section 17) lives in `crates/test-support` per
  the spec's own crate-ownership table, is always constructed (cheap when
  unconfigured), and is runtime-disabled by only mounting
  `/_test/faults/*` when `FAILURE_INJECTION_ENABLED=true`. Startup fails
  fast if that flag is set with `ENVIRONMENT=production`. See
  `docs/adr/0004-fault-injector-placement-and-control.md`.
- `DELIVERY_MODE` default changed from `outbox` (M00's placeholder) to
  `naive` (M02) — `outbox` parses but has no implementation until M03,
  per spec section 11's "defaulting to outbox" instruction, which only
  applies "after outbox introduction."

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

## Commands run and results (M02)

All commands below were actually run in this repository state on
2026-08-19, against the M00/M01 infrastructure (still running). Full
transcript excerpts, including both dual-write gate reproductions, are in
`docs/evidence/m02.md`.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `cargo build --workspace` | success |
| `cargo test -p messaging -- --ignored` | 1/1 (live publish to real Redpanda, confirmed via `rpk topic consume`) |
| `make test-unit` | all crates pass (orders 14/14, payments 2/2, test-support 5/5, contracts 2/2, domain-common 3/3) |
| `cargo test -p orders --test http_tests --test repository_tests --test dual_write_tests` | 16/16 (7 + 7 + 2) |
| `make demo-naive-failure` | exit 0; both gaps reproduced with real Postgres/Kafka evidence |

Total orders-service tests: 26 passed, 0 failed (24 from M01 unchanged +
2 new dual-write demonstrations).

## Next action

Start M03: transactional outbox. Add `outbox_events` table/migration
(spec section 9), rewrite `create_order` to insert the business row and
the outbox envelope in the same transaction, build the claim-lease
publisher worker (`FOR UPDATE SKIP LOCKED`, spec section 13), and wire
`DELIVERY_MODE=outbox` as a second real code path alongside `naive`
(which must keep working — the M02 failure lab stays runnable per spec
section 11's closing paragraph). Do not remove or weaken
`dual_write_tests.rs`.
