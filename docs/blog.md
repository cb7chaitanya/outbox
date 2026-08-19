# From dual writes to effectively-once effects

The first design committed an order to Postgres and then published an event.
A fault between those lines left an order the broker never saw. Publishing
first only inverted the inconsistency; retrying an ambiguous response created
a duplicate. Two independently committed systems do not become atomic because
the caller retries harder.

The transactional outbox moved the boundary. Business state and serialized
events commit locally together. A leased publisher may crash and republish, so
delivery remains at least once. Reliability comes from idempotent effects.

Consumers claim `(consumer,event_id)`, verify payload hashes, enforce semantic
versions, mutate state, and emit their next event in one transaction. Offsets
advance afterward. The fake provider keeps its own idempotency ledger because
an inbox cannot prevent a duplicate external effect after response loss.

Choreography makes ownership explicit: orders decides lifecycle, inventory
owns stock, payments owns money, fulfilment owns shipment creation. Orders
stores independent facts and derives readiness rather than asking services to
infer commands from unrelated events.

Compensation is not rollback. A fulfilment failure requires both refund and
release, and `CANCELLED` is dishonest until both confirmations arrive.
Exhaustion becomes `MANUAL_REVIEW`, exposing stranded money or stock.

Broker order and semantic order also differ. Each downstream relationship has
its own command sequence. Consumers recover gaps within a fetched window and
commit only a contiguous broker-offset prefix, so reordered handling cannot
skip work after a crash. Unresolved gaps become explicit DLQ records.

Replay preserves event identity, allowing inbox and provider idempotency to do
their jobs. Recovery metadata belongs in headers. Operators correct the cause
before replay; replay is not diagnosis.

Finally, correctness needs evidence under broker outage, database outage,
worker death, poison records, duplicates, response loss, and compensation
failure. Metrics expose backlog age, lag, retries, DLQ traffic, and
compensation age; correlation reconstructs a journey without putting IDs into
metric labels.

The result is not magical exactly-once delivery. It is at-least-once transport,
atomic local changes, and effectively-once business effects backed by tests,
fault injection, and a runbook.

## Interview prompts

- Why can neither ordering of DB write and broker publish close dual-write?
- Where is each atomicity boundary and what remains at least once?
- Why must an external provider honor idempotency independently?
- Why is cancellation unsafe before compensation acknowledgements?
- How does contiguous offset tracking make reordered handling crash-safe?
- When would orchestration be preferable, and what coupling would it add?
