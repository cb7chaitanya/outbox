# ADR 0010: Orders consumes reservation outcomes now; compensation waits for M06

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M05 (scope boundary with M06)

## Context

M05's job is the payments service. But payments has nothing to consume
until something emits `payments.authorize_payment`, and spec section 12's
choreography-first happy path is explicit about who does that and when:
"Payment authorization begins only after reservation succeeds." Section
6's service-boundary table gives orders, not inventory or payments, the
"cancellation/compensation requests" role — orders is the aggregate that
reacts to outcomes and decides what happens next.

So M05 genuinely needs orders to consume `inventory.reservation_succeeded`
/ `inventory.reservation_failed` from `inventory.events.v1`, react, and
(on success) emit `authorize_payment`. The question this ADR answers is
how much reaction to build now versus leave for M06 ("Choreographed
workflow and compensation," which owns the full compensation matrix from
section 12: inventory release, payment refund, cancellation convergence).

## Decision

Orders gets a new consumer (`outcome_consumer`, its own inbox/outbox
transaction, same eight-step protocol as M04's `inventory::consumer` and
M05's `payments::consumer`) that:

- On `reservation_succeeded`: transitions the order `PENDING` →
  `INVENTORY_RESERVED` and emits `payments.authorize_payment` in the same
  transaction (`repository::transition_order_with_outbox`).
- On `reservation_failed`: does **nothing** beyond committing the inbox
  mark (so the message doesn't block or retry forever). No state
  transition, no compensation. A log line notes the gap explicitly.

## Rationale

This is the same shape of scope call ADR 0007 and ADR 0008 already made
for M04: build only what the current milestone's acceptance actually
needs, name the boundary explicitly, and let the milestone that owns the
rest (M06, by spec section 20's own milestone list) build it for real
with its own acceptance evidence — rather than half-building
compensation now and having M06 either redo it or paper over gaps.

Wiring only the success half is deliberately *not* symmetric, and that
asymmetry is the point: a fake "cancel on failure" path built without
M06's actual compensation matrix (release inventory, no payment attempted
per the matrix's first row) would either be wrong (an incomplete
cancellation that leaves inventory reserved forever) or would quietly
become the real compensation implementation without M06's dedicated
tests proving it. Section 1's progression rule — "Do not silently replace
an educational intermediate design with the final design" — cuts the same
way here: an intermediate, honestly-incomplete failure path is fine;
a half-built one pretending to be complete is not.

## Alternatives considered

- **Have a test harness publish `authorize_payment` directly, skip wiring
  orders at all.** Rejected: this was offered as a fallback if the real
  wiring turned out to be a rabbit hole. It wasn't — `transition_order`
  already existed from M01, and `outcome_consumer` is a straightforward
  application of M04's established consumer pattern. Real wiring keeps
  the system live end-to-end, which is exactly what let this milestone
  catch the version-numbering bug below through actual multi-service
  testing rather than isolated unit tests.
- **Build the full compensation matrix now.** Rejected per the Rationale
  above — that's M06's named deliverable with its own acceptance gates.

## A bug this wiring surfaced

The first end-to-end run (real orders + inventory + payments processes, a
real HTTP order) DLQ'd `authorize_payment` as `EXPECTED_VERSION_GAP`. The
command's envelope carried the order's own version at the moment of
transition (2, since `PENDING`→`INVENTORY_RESERVED` had just happened),
but payments' `consumer_aggregate_versions` row for that order didn't
exist yet — its expected next version was 1, not 2, because payments
never receives a message at the order's version-1 milestone (that one,
`reserve_inventory`, went to inventory instead). Each consumer's gapless
version-ordering check (spec section 14) is scoped to *what that consumer
actually receives*, not the order aggregate's global version — reusing
the global version as if it meant the former was the bug.

The fix: `authorize_payment`'s envelope now carries a fixed
`aggregate_version: 1`, correct because it is, in this milestone's scope,
the only command orders ever sends to payments for a given order. This is
a real limitation, not just a note: M06 introduces `refund_payment` as a
*second* command on the same orders→payments relationship, and a fixed
`1` will then make that second command look stale (`1 <= last_version 1`)
rather than being applied. M06 must replace the constant with a real
per-order, per-downstream-consumer counter (e.g. a small table tracking
"how many commands has orders sent to payments for this order") before
adding the refund command. Recorded here and in `docs/progress.md` as an
explicit M06 prerequisite.

## Consequences

- The happy path is genuinely live end-to-end as of M05: a real
  `POST /v1/orders` reaches `INVENTORY_RESERVED` and an `AUTHORIZED`
  payment row within about a second, verified manually against the
  running Compose stack (see `docs/evidence/m05.md`).
- `reservation_failed` orders are inert past `PENDING` until M06 lands —
  documented, not hidden, in `docs/progress.md`'s M06 prerequisites.
- The per-downstream-consumer version-counter gap above must be resolved
  before M06 adds `refund_payment` as a second command on the same
  relationship.
