# ADR 0007: Orders emits `reserve_inventory` explicitly, ahead of M06

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M04

## Context

M04 ("inventory consumer and idempotent inbox") needs a real command to
consume, apply idempotently, and demonstrate DLQ/ordering behavior
against. But the spec's own milestone sequencing puts full choreography
wiring — orders, inventory, and payments actually driving each other
through events — at M06. Through M03, orders only ever produced
`orders.order_created`.

Two paths were available for M04:

1. Have inventory consume `orders.order_created` directly off
   `orders.events.v1` and treat it as an implicit trigger to reserve,
   deferring the "real" `inventory.commands.v1` / `reserve_inventory`
   command wiring to M06.
2. Have orders emit the explicit `inventory.reserve_inventory` command
   (section 8's actual contract) now, alongside `order_created`, in the
   same outbox transaction.

## Decision

Option 2. `services/orders/src/repository.rs`'s `OutboxEventBuilder` was
generalized from returning `Option<NewOutboxEvent>` to `Vec<NewOutboxEvent>`
(a mechanical, behavior-preserving change verified by the existing M01-M03
test suite still passing unmodified in substance — only closure signatures
changed), and `http.rs`'s `create_order` handler now builds both events
inside the same closure, in the same database transaction as the order
row. Both rows share `(aggregate_type='order', aggregate_id=order_id,
aggregate_version=order_version)` but differ in `topic`, which the outbox
table's uniqueness constraint already accommodates
(`(aggregate_type, aggregate_id, aggregate_version, topic)`).

## Alternatives considered

- **Option 1 (implicit trigger off `order_created`).** Rejected: it builds
  something M06 would have to tear out and rebuild correctly, contradicts
  section 6's explicit service-boundary table (inventory's documented
  input is `inventory.reserve_inventory` commands on
  `inventory.commands.v1`, not orders' own event stream), and blurs the
  event-vs-command distinction section 6 draws a hard line around
  ("Events are facts in past tense; commands are imperative. Do not mix
  the two in one type."). Consuming `order_created` as a trigger would be
  exactly that mixing.
- **Add a `reserve_inventory` emission but make it a *separate* API call or
  a separate outbox insert outside the create-order transaction.** Rejected
  outright — that's a second, narrower dual-write bug, reintroducing
  exactly the atomicity gap M02/M03 exist to eliminate.

## Consequences

- M04's consumer is built against the real, final command contract from
  day one — no rework expected when M06 formalizes choreography.
- M06 inherits a head start: the order→inventory leg of the workflow
  already exists and is tested. M06's actual new work is the
  inventory→orders return leg (orders reacting to
  `reservation_succeeded`/`reservation_failed`) and the payments leg, plus
  the state-machine wiring that turns those facts into order-status
  transitions per section 12.
- `orders`' outbox now always writes two rows per created order under
  `DELIVERY_MODE=outbox` (never under `naive`, which is unaffected — the
  naive path's closure still returns nothing, since it publishes directly
  outside any transaction and predates this ADR entirely). This roughly
  doubles that service's per-request outbox write volume; not a concern
  at this project's scale, and consistent with the outbox's job of
  fanning out every event a committed mutation requires (invariant I3).
