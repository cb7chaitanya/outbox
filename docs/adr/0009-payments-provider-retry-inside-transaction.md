# ADR 0009: The provider retry loop runs inside the inbox transaction

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M05

## Context

Spec section 13's publisher rule is explicit: "claim rows with a lease and
commit the claim quickly; do not hold a DB transaction open during network
I/O." That rule exists because the outbox publisher talks to a real
broker over a real network, and a stalled connection there must not pin a
database transaction open.

M05's authorize handler calls the fake payment provider inside a bounded,
full-jitter-backoff retry loop (spec section 15) to prove "timeout then
success authorizes once" and "lost success response then retry creates
one provider operation." The question is whether that retry loop —
which can sleep for real time between attempts — belongs inside the same
transaction as the inbox claim, mirroring M04's single-transaction
consumer shape, or outside it, mirroring the publisher's "never hold a
transaction open during I/O" discipline.

## Decision

The retry loop runs inside the same transaction as the inbox claim,
version check, business mutation, and outbox insert — i.e. `handle_one`
keeps M04's exact single-transaction shape; `handle_authorize` is just
one more step inside it, not a new transaction boundary.

## Rationale

The publisher's rule is about *real* broker network I/O with unbounded,
unpredictable latency. `FakeProvider` is in-process, has no real network
call, and its only "latency" is an artificial, test-controlled
`delay_ms` on a `FaultInjector` fault (typically unset, i.e. zero) plus
the retry loop's own backoff sleeps (bounded: `base=100ms`,
`cap=30s`, `max_attempts=8`). This project treats that as fast, bounded
application logic, not I/O in the sense section 13's rule is protecting
against.

The alternative — claim the inbox row, commit, call the provider outside
any transaction, then open a second transaction to persist the outcome —
introduces a genuine new problem this project would then have to solve:
what happens if the process crashes between the claim-commit and the
finalize-commit? The inbox row would exist with `processed_at` still
`NULL`, and a redelivery of that exact event id would need to *resume*
processing rather than either re-claiming (impossible, the row exists)
or short-circuiting as already-done (wrong, the reaction never happened).
That's real, non-trivial design surface with its own crash-recovery
model, not something to half-build here.

## Consequences

- `handle_one`'s crash-recovery model for payments is byte-for-byte
  M04's: a crash anywhere before the transaction's `COMMIT` loses
  nothing (transaction never happened); a crash after `COMMIT` but
  before the offset ledger advances is a safe no-op on redelivery via the
  inbox row. No new partial-completion window is introduced.
- A real, network-calling payment provider integration (out of this
  project's scope — section 4's non-goals rule out real payment-provider
  integration) would need to revisit this decision and adopt something
  closer to the publisher's claim-then-release-then-finalize shape.
- The retry loop's worst case (8 attempts, up to 30s backoff each, capped
  at 10 minutes total per spec section 15 defaults) can hold a database
  connection for a long time under sustained provider unavailability.
  Acceptable for this project's connection-pool scale (10 connections,
  local dev/test traffic); would not be acceptable at production scale
  with a real provider, which is exactly why this ADR scopes the decision
  to the fake provider specifically.
