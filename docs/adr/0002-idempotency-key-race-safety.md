# ADR 0002: Race-safe idempotency-key handling via ON CONFLICT DO NOTHING

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M01

## Context

`POST /v1/orders` must satisfy two requirements at once (spec section 10):
concurrent identical requests carrying the same `Idempotency-Key` must
produce exactly one order and return the same response to every caller,
while the same key reused with a materially different request body must
be rejected with `409 IDEMPOTENCY_KEY_REUSED`. A naive "check, then
insert" implementation (`SELECT ... WHERE idempotency_key = $1`, and
insert only if absent) is a classic TOCTOU race: two concurrent requests
can both pass the SELECT before either commits an INSERT, producing two
orders for one logical request — exactly the kind of bug this project
exists to eliminate.

## Decision

`orders` has a database-level `unique` constraint on `idempotency_key`.
`create_order` always attempts
`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING ...` first.
Postgres serializes concurrent inserts on the same unique key at the index
level: the second transaction blocks until the first commits or aborts,
then either sees its own row inserted (if the first aborted) or gets zero
rows back (if the first committed). A caller that gets zero rows back can
therefore safely re-read the row by `idempotency_key` afterward and know
it reflects a fully committed insert — no polling, no retry loop, no
second unique-violation error to catch.

The fingerprint used to detect a reused key with a different body is a
SHA-256 hash of a canonical JSON structure (currency plus items sorted by
SKU), stored in `idempotency_request_hash` on the same row. Sorting items
before hashing means requests that differ only in item ordering are
treated as byte-equivalent, matching the spirit of "byte-equivalent
normalized request" in section 10 without requiring literal byte-identical
request bodies.

## Alternatives considered

- **Check-then-insert with `SELECT ... FOR UPDATE` on a synthetic lock
  row.** Works, but requires a separate advisory-lock or lock-table
  mechanism keyed by a value that doesn't exist until the first insert.
  More moving parts for no benefit over letting Postgres's own unique
  index do the serialization.
- **Application-level mutex/lock service (e.g. Redis) keyed by
  idempotency key.** Adds an external dependency and a second source of
  truth that could disagree with the database after a crash. Rejected in
  favor of relying on the constraint the database already enforces
  durably.
- **`INSERT ... ON CONFLICT (idempotency_key) DO UPDATE` with a
  no-op-if-unchanged clause.** Considered so a single statement could both
  detect and reject a body mismatch, but `DO UPDATE` requires deciding
  which columns to touch even when nothing should change, and mixing
  business-conflict detection into the conflict clause makes the SQL
  harder to reason about than a follow-up read. `DO NOTHING` plus a
  read-and-compare is simpler to audit.

## Consequences

- Order creation always begins with one INSERT attempt regardless of
  whether the key is new or reused; the common case (new key) pays no
  extra round trip. The reused-key case pays one extra SELECT, which is
  acceptable since the client is being rejected or replayed, not being
  billed for retry cost.
- The race-safety argument depends on Postgres's documented behavior for
  concurrent `INSERT ... ON CONFLICT` on a unique index (see repository
  tests: `concurrent_identical_idempotent_requests_yield_one_order`,
  `same_key_same_body_replays_original_order`). This is Postgres-specific
  behavior; porting the outbox/inbox pattern to a different database in a
  later milestone would need to re-verify this guarantee.
- The hash column is internal-only (never serialized in the HTTP
  representation); it exists purely to distinguish "replay" from "key
  reused with a different request" without storing or diffing full
  request bodies.
