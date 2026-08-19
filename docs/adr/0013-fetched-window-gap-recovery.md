# ADR 0013: Recover version gaps inside a fetched window

- Status: Accepted
- Date: 2026-08-19

## Context

Consumers previously sent every version gap directly to DLQ. That was safe
but did not satisfy the bounded recovery policy. Processing a reordered batch
naively is also unsafe: committing a higher broker offset first can skip an
unprocessed lower offset after a crash.

## Decision

Every consumer orders a fetched window by aggregate ID and aggregate version.
This lets a later broker record containing the missing version run before the
gapped record. Malformed records retain deterministic offset order and still
use the poison path.

A shared contiguous-offset tracker records completed broker offsets but only
exposes an acknowledgement when every lower offset is complete. A crash may
therefore cause harmless replay, never skipped work. Gaps not recoverable from
the bounded fetch window retain the stable `EXPECTED_VERSION_GAP` DLQ policy.

## Consequences

- Unrelated aggregates in the same fetch window continue progressing.
- Recoverable reordering avoids DLQ without weakening acknowledgement safety.
- The fetch size and `max_wait_ms` form the bounded recovery window.
- A future multi-partition implementation needs one tracker per partition.
