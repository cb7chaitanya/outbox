# ADR 0005: Outbox claim leases, event-builder closures, and backoff scope

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M03

## Context

Spec section 13 requires the outbox insert to happen in the same
transaction as the business mutation, and a separate publisher worker to
claim rows with `FOR UPDATE SKIP LOCKED`, publish independently of the
request path, and retry with backoff on failure. Several implementation
choices weren't fully specified and needed a decision:

1. How does the repository, which generates the order's id internally,
   let the HTTP layer (which owns the concrete event type from
   `contracts`) build an outbox envelope that needs that id — without the
   `persistence` crate or `orders::repository` depending on `contracts`?
2. How does a claimed-but-not-yet-published row distinguish "claimed for
   the first time" from "reclaimed after a lease expired," which spec
   section 16 wants as a distinct `lease_recoveries` metric?
3. How much of section 15's full retry taxonomy (transient vs. permanent
   vs. poison classification, seeded RNG, configurable budgets) does the
   M03 publisher need, given section 15 itself is a later milestone
   (M05)?
4. How to test "broker outage grows the backlog, recovery drains it"
   without a slow, flaky `docker compose stop redpanda` in the hot test
   path?

## Decision

1. **Outbox event builder as a closure.** `repository::create_order` now
   takes `build_outbox_event: impl FnOnce(Uuid, i64) -> Option<NewOutboxEvent>`,
   invoked with the order's id and version only *after* the order row is
   inserted, and only when a new row was genuinely created (never on an
   idempotent replay). The closure is built in `http.rs` from the
   pre-validated request plus a `contracts::Envelope`, keeping
   `persistence` and `orders::repository` free of any dependency on
   concrete event payload types. `persistence::outbox::insert` only ever
   sees the generic `serde_json::Value` envelope.
2. **`was_previously_claimed` returned from the claim query itself.** The
   `claim_batch` query's CTE captures the row's `claimed_by` value
   *before* overwriting it, and returns `(previous_claimed_by IS NOT
   NULL)` as part of the same atomic claim — no separate read, no race
   between "check if claimed" and "claim it."
3. **A real but intentionally narrow backoff/retry implementation now.**
   `full_jitter_backoff` implements the exact formula from section 15
   (`random(0, min(cap, base * 2^attempts))`) with configurable
   base/cap, satisfying M03's "not a fixed sleep" requirement — but
   error classification (transient/permanent/poison), retry budgets, and
   a seeded RNG for deterministic test timing are explicitly deferred to
   M05, which owns that taxonomy for every service, not just orders'
   outbox publisher. M03's publisher currently retries every failure the
   same way; that's acceptable because in M03 the only failure mode is
   "the broker rejected the publish," which is uniformly transient here.
4. **A swappable `Producer` test double instead of stopping the real
   broker.** `FlakyProducer` (in `outbox_tests.rs`) fails while "down"
   and succeeds while "up," driven by an `AtomicBool` the test flips
   directly. This keeps the outage/recovery test fast (tens of
   milliseconds, not container-stop/start latency) and deterministic,
   while still exercising the real claim/backoff/retry code path against
   a real Postgres. `docker compose stop redpanda` remains available for
   a slower, more end-to-end chaos check in M09.

## Alternatives considered

- **Generate the order id in the HTTP layer and pass it into
  `create_order`.** Rejected: it would spread order-id generation across
  two layers for no benefit, and every M01 test already relies on the
  repository owning id generation.
- **Give `persistence` a dependency on `contracts`** so it could build
  typed envelopes itself. Rejected: `persistence` is meant to be reusable
  by every future service (inventory, payments, fulfilment), each with
  its own event types; coupling it to `contracts::orders` specifically
  would defeat that.
- **A second `claimed_by IS NOT NULL` read before claiming.** Rejected:
  introduces a TOCTOU gap between the check and the claim under
  concurrent workers; folding the check into the same `UPDATE ...
  RETURNING` is both simpler and race-free.
- **Full section-15 retry taxonomy now.** Rejected as premature: M03 has
  exactly one failure source (publish to Kafka); building a general
  classifier ahead of the services that will actually need to
  distinguish transient/permanent/poison (M04 inbox, M05 payments) would
  be speculative.
- **Stop/start the real Redpanda container in the outage test.**
  Rejected for the automated suite: adds seconds of latency and
  occasional flakiness from container health-check timing to every test
  run. Kept as a documented option for manual/chaos verification instead.

## Consequences

- Adding a second producing event type (e.g. a future `orders` command)
  only requires a new closure at the call site; no change to
  `persistence` or the publisher loop.
- `lease_recoveries_total` is exact, not sampled or inferred after the
  fact, because it's computed atomically with the claim.
- M05 will need to revisit `run_publisher_once`'s single `Err ->
  mark_failed` branch once permanent/poison failures exist for the
  outbox path (today, every producer error is treated as retryable,
  which is correct only because Kafka-publish failures in this project
  are transport-level).
- The outage test's realism is bounded by `FlakyProducer` only
  simulating publish failure, not e.g. partial writes or broker-side
  duplicate suppression; that's acceptable for what M03 needs to prove
  (backlog grows, then drains) and is explicitly out of scope here.
