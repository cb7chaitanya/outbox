# ADR 0008: M04 resolves a version gap immediately; bounded retry is M08

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M04 (scope boundary with M08)

## Context

Spec section 14's ordering/replay policy has three outcomes for an
incoming event's aggregate version against a consumer's last-applied
version: apply (exactly `last + 1`), stale (`<= last`, acknowledge as a
harmless duplicate), or gap (`> last + 1`) — the last of which the spec
says should "retry/buffer for a bounded interval without blocking
unrelated keys, then DLQ with `EXPECTED_VERSION_GAP` if unresolved."

M04's acceptance gates don't require a gap scenario at all — the five
required demonstrations are duplicate delivery, oversell prevention,
all-or-nothing multi-SKU, crash-before-offset-commit, and DLQ poison
isolation. But `persistence::inbox::version_decision` (built for this
milestone, shared infrastructure per spec section 7) already classifies
`Gap` as a distinct case from `Stale`, and section 20's M08 ("Ordering,
replay, and concurrency hardening") is explicitly the milestone that
"recovers within budget or goes to DLQ with expected code" — i.e. the
bounded retry/buffer window is M08's actual deliverable, not M04's.

## Decision

`inventory::consumer::handle_one` treats `VersionDecision::Gap` as an
immediate DLQ with error code `EXPECTED_VERSION_GAP` — no retry window,
no buffering. The inbox row is still committed (not rolled back) so a
redelivery of the same `event_id` short-circuits as a duplicate rather
than producing a second DLQ record for the same message.

## Alternatives considered

- **Implement the full bounded retry/buffer window now.** Would require
  a redelivery/backoff mechanism for out-of-order aggregate versions
  distinct from the outbox's own retry machinery (this is inbound
  ordering, not outbound publish failure), plus a place to park a
  buffered-but-not-yet-appliable event. That's real, non-trivial design
  surface that section 20 assigns to M08 by name. Building it now would
  either be thrown away and rebuilt to M08's actual design, or would
  informally complete M08 without its own acceptance evidence — exactly
  what section 1's progression rule warns against ("Do not silently
  replace an educational intermediate design with the final design").
- **Silently apply out-of-order events anyway.** Rejected outright: this
  is precisely invariant I13 ("stale-event safety... cannot regress
  state") and I12 (version monotonicity) territory. Applying version 5
  when only version 2 has been seen risks acting on facts whose
  prerequisites were never recorded.

## Consequences

- A gap scenario in this project (as of M04) always ends in DLQ,
  immediately, with no recovery window — operationally blunter than the
  spec's final target, but honest about it: the error code
  (`EXPECTED_VERSION_GAP`) is already the one M08 keeps.
- M08 is the milestone responsible for adding the actual bounded
  retry/buffer step *before* the DLQ branch here fires, plus a targeted
  test proving a bounded-recoverable gap (e.g. redelivered out of order by
  the broker, corrected before the bound expires) no longer immediately
  DLQs. `docs/progress.md` names this as an explicit M08 prerequisite.
- Every other M04 acceptance path (duplicate, apply, stale, poison) is
  fully implemented per spec with no shortcut — this boundary is scoped
  narrowly to the gap branch alone.
