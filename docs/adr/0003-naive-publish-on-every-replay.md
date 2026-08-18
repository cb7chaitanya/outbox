# ADR 0003: The naive publish path republishes on every accepted request, including idempotent replays

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M02

## Context

M01's idempotency layer distinguishes a genuine create (`created: true`)
from a replay of an already-committed order (`created: false`) — a
retried `POST /v1/orders` with the same `Idempotency-Key` never inserts a
second row. M02 adds a direct-to-Kafka publish after the DB step (spec
section 11), and had to decide what the naive publish code does on a
replay: publish again, or skip it because "we already handled this key"?

## Decision

The naive path (`publish_naive` in `services/orders/src/http.rs`)
publishes `orders.order_created` on **every** accepted `POST /v1/orders`
call, whether or not this specific call was the one that inserted the
row. It does not consult `outcome.created` to decide whether to publish —
only to decide whether the `orders.after_db_commit_before_publish` fault
point applies (that fault is specifically about the gap between *this
call's* commit and *this call's* publish, so it's meaningless on a replay
where no commit happened).

## Rationale

This is deliberately the naive, uncoordinated behavior, not an oversight.
Skipping the publish on replay would require the publish decision to
consult the same idempotency state the DB write already consulted —
which is exactly the coordination the transactional outbox (M03)
provides for real. Building that coordination into the M02 naive path
would quietly turn it into a half-outbox and hide the milestone's actual
lesson: **a bolted-on publish step, with no shared transaction and no
shared idempotency ledger with the write it's supposed to accompany, does
the wrong thing under retry.**

Concretely, this choice is what makes
`dual_write_publish_then_retry_duplicate` (`services/orders/tests/
dual_write_tests.rs`) a real duplicate-delivery demonstration rather than
a hedge. With the `orders.after_publish_before_response` fault
configured: the first request commits the order, publishes successfully,
then the client sees an injected failure; a naive client retries with the
same key; the DB layer correctly replays (no second row) but the publish
step fires again regardless, because nothing told it not to. The broker
ends up with two distinct events (`event_id` differs) for the same
`order_id` — this is a real, observable duplicate on the broker, not a
client-side illusion.

## Consequences

- Every accepted create call — first or replayed — costs a Kafka publish
  in naive mode. This is expected to be worse in naive mode than in
  outbox mode; M03 is expected to fix it by publishing exactly once per
  logical mutation via the outbox row's own uniqueness constraint, not by
  teaching the naive path to be smarter.
- Any consumer of `orders.events.v1` must already be idempotent (spec
  section 14, invariant I11) even before M03 exists — this project never
  has a period where duplicate delivery isn't possible.
- If a future milestone needs a genuinely "publish only on the winning
  call" naive variant for comparison, add it as a clearly-named third
  code path rather than modifying this one — the whole point of this ADR
  is that this path stays naive.
