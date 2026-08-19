# ADR 0011: A real per-(order, downstream target) command-version counter

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M06

## Context

ADR 0010 (M05) shipped a fixed `aggregate_version: 1` for the
`authorize_payment` command orders sends to payments, flagging it as a
known limitation: correct only because `authorize_payment` was, at the
time, the *only* command orders ever sent to payments for a given order.
M06 breaks that assumption twice over: `refund_payment` becomes a second
command on the orders->payments relationship (scaffolded here, wired fully
in M07), and `release_inventory` becomes a second command on the
orders->inventory relationship (alongside the existing `reserve_inventory`,
also fixed at a hardcoded `order_version` that happened to equal 1 for the
same reason).

Each downstream consumer's gapless version-ordering check (spec section
14) is scoped to *what that consumer actually receives* from orders, not
the order aggregate's own global version. Reusing the order's version, or
a constant, for a second command on the same relationship makes that
second command look stale or gapped to a consumer that correctly expects
`last_version + 1`.

## Decision

A new table, `outbound_command_sequences(order_id, target, next_version)`,
tracks orders' own outbound command count per `(order_id, target)` pair
(`target` is a downstream service name: `"inventory"`, `"payments"`).
`repository::reserve_command_version` atomically reserves and increments
the next version for a given pair in one `INSERT ... ON CONFLICT DO
UPDATE ... RETURNING` statement, and must run in the *same* transaction as
the outbox insert the reserved version will be stamped on -- reserving it
outside that transaction would let a rolled-back attempt permanently skip
a version number, which (unlike an ordinary gap that resolves once the
missing message arrives) can never be filled, since nothing will ever
carry that exact version again.

Each call site that emits a downstream command reserves its own version
using the transaction connection it already holds, immediately before
building that command's envelope:

- `repository::create_order` reserves `"inventory"`'s version internally
  (its transaction is not exposed to the caller) and passes it to a new,
  `create_order`-specific 3-argument outbox-builder closure signature
  (`CreateOrderOutboxEventBuilder`) rather than widening the existing
  2-argument `OutboxEventBuilder` trait that `transition_order_with_outbox`
  still uses unchanged.
- Callers of `transition_order_with_outbox` (the outcome consumer) already
  hold the transaction `transition_order_with_outbox` itself uses, so they
  reserve whatever version they need directly and capture it into their
  closure by ordinary move-capture -- no signature change needed there at
  all.

## Alternatives considered

- **Widen `OutboxEventBuilder` to carry every possible target's version.**
  Rejected: a call site that reserves a version it doesn't end up using
  permanently wastes that slot in the target's sequence (the target
  consumer will then see a real, later command's version as a gap it can
  never resolve, since the skipped version was never actually sent).
  Reserving only the target(s) a given call site actually emits to avoids
  this.
- **Derive the command version from the order's own `version` column.**
  This is exactly what M05 did (and what `reserve_inventory` did,
  unnoticed, until this milestone needed a second inventory-bound
  command) -- rejected for the reason above: two independent downstream
  relationships cannot both be indexed by one shared counter without one
  of them seeing gaps.

## Consequences

- `release_inventory`, added this milestone, is provably safe against the
  same class of bug M05's live check caught for `authorize_payment` --
  verified by `choreography_tests.rs`'s
  `payment_failure_releases_inventory_then_cancels`, which exercises the
  real second-command-on-`"inventory"` case end to end.
- `repository::transition_order_with_outbox` grew an 8th parameter
  (`reservation_id: Option<Uuid>`, unrelated to this ADR but landed in the
  same milestone) and now carries `#[allow(clippy::too_many_arguments)]`,
  matching the precedent already set for `build_order_created_envelope` in
  `http.rs`.
- A future consumer relationship (e.g. if fulfilment ever needed a command
  from orders) reuses this same primitive without new design work.
