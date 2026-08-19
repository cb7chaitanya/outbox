# Progress

## Milestone checklist

- [x] M00 — Repository contract and skeleton
- [x] M01 — Orders API and local consistency
- [x] M02 — Naive dual write and failure lab
- [x] M03 — Transactional outbox
- [ ] M04 — Inventory consumer and idempotent inbox
- [ ] M05 — Payments with retry taxonomy
- [ ] M06 — Choreographed workflow and compensation
- [ ] M07 — Fulfilment and complete compensation matrix
- [ ] M08 — Ordering, replay, and concurrency hardening
- [ ] M09 — Observability, chaos, and operations
- [ ] M10 — Final acceptance, README, and learning write-up
- [ ] M11 — Optional saga orchestrator (only after M10)

## Current milestone

M03 complete. Next action: M04 — inventory consumer and idempotent inbox.

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
- M03 flips `DELIVERY_MODE`'s default back to `outbox`, now with a real
  implementation, per spec section 11's closing paragraph. `naive` stays
  fully runnable and its M02 tests/demo script are untouched.
- The outbox event builder is passed into `repository::create_order` as
  an `impl FnOnce(Uuid, i64) -> Option<NewOutboxEvent>` closure, invoked
  with the order's id/version only when a row is genuinely created (never
  on a replay). This keeps `persistence` and `orders::repository` free of
  any dependency on concrete event payload types from `contracts`. See
  `docs/adr/0005-outbox-claim-lease-and-backoff-design.md`.
- `claim_batch`'s query returns `was_previously_claimed` computed inside
  the same atomic `UPDATE ... RETURNING`, so the "lease recovery" metric
  (spec section 16) is exact, not inferred after the fact.
- M03's publisher implements the exact full-jitter backoff formula from
  spec section 15 but not that section's full error-classification/retry-
  budget taxonomy — deferred to M05, which owns that taxonomy for every
  service. Documented as an explicit scope boundary in ADR 0005.
- The broker-outage acceptance test uses a `FlakyProducer` test double
  (an `AtomicBool` toggle) rather than stopping the real Redpanda
  container, for test speed and determinism; `docker compose stop
  redpanda` remains available for slower end-to-end chaos checks in M09.
- Outbox publisher runs unconditionally from `main.rs` regardless of
  `delivery_mode` (a no-op poll loop when the table is empty in naive
  mode), so toggling `DELIVERY_MODE` back to `outbox` never needs a
  restart-time wiring change.

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

## Commands run and results (M03)

All commands below were actually run in this repository state on
2026-08-19, against the M00-M02 infrastructure (still running). Full
transcript excerpts, including all six M03 acceptance-gate proofs and a
live end-to-end `/metrics` check, are in `docs/evidence/m03.md`.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `make test` (fmt + lint + unit + integration) | all green |
| `cargo test -p orders --test outbox_tests` | 6/6, all six M03 acceptance gates |
| same, repeated 5x with `--test-threads=4` | 6/6 every run, no flakes |
| `make demo-naive-failure` | exit 0; M02 lab still reproduces both gaps unchanged |
| `cargo run -p orders` + curl create + `/metrics` | live order published end-to-end via the background publisher; `outbox_unpublished_count 0`, `outbox_publish_success_total 1` |

Total orders-service tests: 34 passed, 0 failed (26 from M01+M02
unchanged + 6 new outbox acceptance demonstrations, plus 3 new
`full_jitter_backoff` unit tests in `persistence`).

## Next action

Start M04: inventory consumer and idempotent inbox. Add the
`inbox_events`/`consumer_aggregate_versions` tables and claim/dedupe
primitives to `persistence` (spec section 9, section 14), build the
inventory service's stock/reservation schema and multi-SKU
sorted-lock-order reservation transaction (spec section 9 "Inventory"),
consume `orders.order_created` from `orders.events.v1` with the
inbox-transaction discipline (validate → insert inbox row with `ON
CONFLICT DO NOTHING` → check hash on duplicate → apply mutation →
insert outbox events → mark processed → commit DB → commit Kafka offset
only after), and reuse this milestone's outbox publisher/backoff code
for inventory's own outbox rather than duplicating it. Do not disable
Kafka auto-commit assumptions loosely — offset commit must happen only
after the local transaction commits.
