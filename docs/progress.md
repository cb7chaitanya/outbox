# Progress

## Milestone checklist

- [x] M00 — Repository contract and skeleton
- [x] M01 — Orders API and local consistency
- [x] M02 — Naive dual write and failure lab
- [x] M03 — Transactional outbox
- [x] M04 — Inventory consumer and idempotent inbox
- [x] M05 — Payments with retry taxonomy
- [x] M06 — Choreographed workflow and compensation
- [x] M07 — Fulfilment and complete compensation matrix
- [ ] M08 — Ordering, replay, and concurrency hardening
- [ ] M09 — Observability, chaos, and operations
- [ ] M10 — Final acceptance, README, and learning write-up
- [ ] M11 — Optional saga orchestrator (only after M10)

## Current milestone

M07 complete. Next action: M08 — ordering, replay, and concurrency hardening.

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
- M04 judgment call: orders now emits `inventory.reserve_inventory`
  explicitly (on `inventory.commands.v1`, same outbox transaction as
  `order_created`) rather than having inventory consume `order_created`
  directly as an implicit trigger — the real command contract from
  section 8, not a placeholder M06 would have to replace. See
  `docs/adr/0007-orders-emits-reserve-inventory-command.md`.
- `rskafka` has no consumer-group/offset-commit protocol (ADR 0001's
  build-toolchain constraint applies here too). Consumers track their own
  read position in a `consumer_offsets` table per service, advanced only
  after the corresponding business transaction (or DLQ publish) commits —
  the same "never ack before durable" guarantee the spec asks for, just
  implemented locally instead of via a broker API. See
  `docs/adr/0006-inbox-consumer-offset-ledger.md`.
- M04 scope boundary: an out-of-order aggregate version (`Gap` in
  `persistence::inbox::version_decision`) goes straight to DLQ with
  `EXPECTED_VERSION_GAP` — no bounded retry/buffer window yet. That
  window is M08's ("Ordering, replay, and concurrency hardening")
  deliverable by name. See `docs/adr/0008-m04-gap-policy-scope-boundary.md`.
  **M08 must add:** the bounded retry/buffer step before this DLQ branch,
  plus a test proving a recoverable gap no longer immediately DLQs.
- Reusable inbox primitives (`persistence::inbox`: `try_claim`, `fetch`,
  `mark_processed`, `version_decision`, `advance_version`,
  `fetch_offset`/`commit_offset`) and the DLQ record shape/publish helper
  (`persistence::dlq`) are generic across every future consumer, matching
  how `persistence::outbox` was already reusable when inventory needed
  its own outbox in this same milestone (no changes to `outbox.rs` were
  needed — it was never orders-coupled).
- Inventory's `repository::reserve` takes an already-open transaction
  connection rather than owning its own transaction (unlike orders'
  `create_order`), because the consumer handler must apply it, insert the
  resulting outbox event, advance the consumer-version row, and mark the
  inbox row processed all in one transaction (spec section 14 steps 4-7).
- Section 15's error-classification taxonomy (`ErrorClass`:
  Transient/Contention/RateLimited/Permanent/Poison) lives in
  `domain-common`, not `persistence` — it's a vocabulary type any
  service's error enum can map onto, not persistence-specific.
- M05 judgment call: orders gets a new consumer (`outcome_consumer`)
  reacting to `inventory.reservation_succeeded`/`reservation_failed` on
  `inventory.events.v1` — transitioning the order and emitting
  `payments.authorize_payment` on success, deliberately doing nothing on
  failure beyond the inbox mark. Full compensation (release inventory,
  drive to `CANCELLING`/`CANCELLED`) is M06's named deliverable. See
  `docs/adr/0010-orders-consumes-reservation-outcomes.md`.
  **M06 must add:** the `reservation_failed` reaction (compensation), and
  a real per-order per-downstream-consumer command-version counter before
  adding `refund_payment` as a second command on the orders→payments
  relationship (see the ADR's "a bug this wiring surfaced" section — the
  current `authorize_payment` envelope hardcodes `aggregate_version: 1`,
  which only stays correct while it's the only command on that
  relationship).
- The fake payment provider (`payments::provider::FakeProvider`) keeps
  its own in-process idempotency-key ledger, independent of the
  `payment_operations` table (this service's own bookkeeping of what it
  asked the provider to do). Three idempotency layers now stack across
  the payments consumer: inbox event-id dedup (redelivery), the
  `payments.order_id` unique constraint (a second command instance for
  the same order), and the provider's own ledger (this handler's own
  retry loop). Each covers a different failure mode.
- The provider retry loop runs inside the same transaction as the inbox
  claim, unlike the outbox publisher's "never hold a transaction open
  during I/O" rule — accepted because the fake provider has no real
  network I/O. See
  `docs/adr/0009-payments-provider-retry-inside-transaction.md`.
- `authorize_payment`/`refund_payment` share one topic
  (`payments.commands.v1`); the consumer dispatches on `event_type`
  rather than running two separate consumer loops, since both need the
  identical eight-step protocol and per-order version tracking.
- M06: a real per-`(order_id, target)` outbound command-version counter
  (`outbound_command_sequences`, `repository::reserve_command_version`)
  replaces the hardcoded versions `reserve_inventory`/`authorize_payment`
  relied on, so `release_inventory` (a second command to `"inventory"`)
  doesn't look stale/gapped to inventory's inbox. See
  `docs/adr/0011-per-target-command-version-counter.md`.
- `orders` gained a `reservation_id` column (nullable, set on
  `reservation_succeeded`) so a later `payment_failed` can build
  `release_inventory`'s payload without a cross-service join (spec
  section 6 forbids one).
- Orders' outcome consumer now handles both `inventory.events.v1` and
  `payments.events.v1` (one module, one consumer name, two
  `process_available` loop instances in `main.rs`, dispatch on
  `event_type` since it's unique across both topics).
- Real bug found by this milestone's own live end-to-end check:
  `TransitionError::NotFound` was propagated as a fatal `Err`, wedging the
  outcome consumer's offset ledger forever on any record naming an order
  the consumer's database has no row for. Fixed to DLQ as `UNKNOWN_ORDER`
  instead (invariant I15) — see `docs/evidence/m06.md` for the full story
  and the regression test.
- M06 scope boundary (compensation matrix rows 3-4, section 12): fulfilment
  failure → refund + release, and compensation retry exhaustion →
  `MANUAL_REVIEW`, are **not** implemented — both depend on the fulfilment
  service, which doesn't exist until M07. Details in `docs/evidence/m06.md`.
  **M07 must add:** fulfilment service itself, the third compensation row
  (refund payment + release inventory together, tracking both
  confirmations before `CANCELLED`), and `MANUAL_REVIEW` on exhausted
  compensation retries.

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

## Commands run and results (M04)

All commands below were actually run in this repository state on
2026-08-19, against the M00-M03 infrastructure (still running). Full
transcript excerpts, including all five M04 acceptance-gate proofs and a
live end-to-end order → reservation check, are in `docs/evidence/m04.md`.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `cargo build --workspace` | success |
| `cargo test -p inventory --lib` | 5/5 |
| `cargo test -p inventory --test consumer_tests` | 5/5, all five M04 acceptance gates |
| same, repeated 3x | 5/5 every run, no flakes |
| `cargo test --workspace --tests` (full suite) | all green across every crate/service |
| live: `cargo run -p orders` + `cargo run -p inventory`, seed stock, `POST /v1/orders`, poll stock | `available_qty` 50→45, `reserved_qty` 0→5 within 3s; `reservation_succeeded` observed on `inventory.events.v1` with correct correlation/causation |

Total inventory-service tests: 10 passed, 0 failed (5 unit + 5
integration). Workspace-wide test count now in the 70s across all
crates/services.

## Commands run and results (M05)

All commands below were actually run in this repository state on
2026-08-19, against the M00-M04 infrastructure (still running). Full
transcript excerpts, including all five M05 acceptance-gate proofs and a
live end-to-end order → reservation → authorization check (plus the
version-gap bug it caught and the fix), are in `docs/evidence/m05.md`.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `cargo build --workspace` | success |
| `cargo test -p payments --lib` | 6/6 (fake provider unit tests) |
| `cargo test -p payments --test consumer_tests` | 5/5, all five M05 acceptance gates |
| same, repeated 3x with `--test-threads=1` | 5/5 every run, no flakes |
| `cargo test -p orders --tests` | 20/20 (no regression from M01-M03) |
| `cargo test -p inventory --tests` | 10/10 (no regression from M04) |
| `make demo-naive-failure` | exit 0; M02 lab still reproduces both gaps unchanged |
| live: `cargo run -p orders/inventory/payments`, seed stock, `POST /v1/orders`, poll order + query `payments` table | `PENDING`→`INVENTORY_RESERVED` in ~1s; one `AUTHORIZED` payments row with a real `provider_reference` |

Total payments-service tests: 11 passed, 0 failed (6 unit + 5
integration). Workspace-wide test count now in the 90s across all
crates/services.

## Commands run and results (M06)

All commands below were actually run in this repository state on
2026-08-19, against the M00-M05 infrastructure (still running). Full
transcript excerpts, including all five M06 acceptance-gate proofs, the
live end-to-end run, and the poison-isolation bug/fix, are in
`docs/evidence/m06.md`.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warnings |
| `cargo test --workspace --tests` (full suite) | all green across every crate/service |
| `cargo test -p orders --test choreography_tests` | 6/6, all five M06 acceptance gates + 1 poison-isolation regression test |
| same, repeated 2x more (default parallelism) + 1x with `--test-threads=1` | 6/6 every run, no flakes |
| `cargo test -p inventory --test release_tests` | 2/2 |
| `cargo test -p orders --tests` (regression check) | 42/42, no regressions from M01-M05 |
| `make demo-naive-failure` | exit 0; M02 lab still reproduces both gaps unchanged |
| live: `cargo run -p orders/inventory/payments`, seed stock, `POST /v1/orders`, poll order | `PENDING`→`INVENTORY_RESERVED`→`PAYMENT_AUTHORIZED` in ~1-2s; real stock decrement (50→47 available, 0→3 reserved); full transition history correct |

Total orders-service tests: 42 passed, 0 failed (34 from M01-M05 unchanged
+ 6 new choreography/compensation acceptance tests + 2 new
`outbound_command_sequences`/`reservation_id` migration coverage via
existing tests). Total inventory-service tests: 14 passed, 0 failed (12
from M04 + 2 new release tests). Workspace-wide test count now over 100
across all crates/services.

## M07 completion

Fulfilment now consumes explicit readiness commands through an idempotent
inbox and emits created/failed outcomes through its transactional outbox.
Orders records reservation/payment facts, emits fulfilment exactly once,
reaches `COMPLETED` on success, and runs refund plus inventory release on
failure. It reaches `CANCELLED` only after both acknowledgements, or
`MANUAL_REVIEW` with a DLQ/operator signal when compensation is exhausted.

Acceptance evidence, including two real four-service flows, is recorded in
`docs/evidence/m07.md`.

## Next action

Start M08: ordering, replay, and concurrency hardening.
