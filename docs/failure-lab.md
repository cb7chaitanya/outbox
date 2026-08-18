# Failure lab: why database-plus-broker dual writes are unsafe

This document explains the two atomicity gaps in the naive dual-write
stage (`DELIVERY_MODE=naive`, spec section 11) and why they are permanent
properties of the approach, not bugs that a smarter retry policy can
paper over. Run `make demo-naive-failure` (or `scripts/
demo-dual-write-failure.sh` directly) to reproduce both gaps against a
live Compose stack; the deterministic, automated versions of the same
demonstrations are `dual_write_db_commit_without_event` and
`dual_write_publish_then_retry_duplicate` in `services/orders/tests/
dual_write_tests.rs`.

## The naive flow

```text
1. INSERT the order, COMMIT the Postgres transaction.
2. Publish orders.order_created directly to Kafka.
3. Return 202 to the client only if both steps succeeded.
```

Two independent systems — a database and a message broker — each commit
their own effect durably, but there is no shared transaction, no shared
log, and no third party watching both. Between step 1 and step 2, and
between step 2 and step 3, the process can fail, and each failure leaves
a *different* kind of inconsistency behind.

## Gap 1 — `orders.after_db_commit_before_publish`

**What happens:** the Postgres transaction commits. The order now exists,
durably, with a client-visible identity (`idempotency_key`). Before the
Kafka publish is even attempted, the process fails (or, in this project's
deterministic fault-injection version, `FaultInjector` intercepts the
call at exactly this point and returns an error instead of publishing).

**What the client sees:** an error response (`503 INJECTED_FAULT` in the
lab; in a real failure it would typically be a connection reset, timeout,
or 5xx from a crashed process).

**What's actually true:** the order is fully committed. `GET
/v1/orders/{id}` — or, as the demo script does, a direct query by
`idempotency_key` — proves it. But `orders.events.v1` has zero matching
records for that order, verified in the lab by reading the entire topic
from the earliest offset and finding no envelope whose `aggregate_id`
matches.

**Why retries cannot close this gap:** there is nothing left to retry.
The client doesn't know the order was created — it saw a failure — so a
naive client might reasonably retry the same request. But this project's
idempotency-key layer (M01) means that retry doesn't create a *second*
order; it just finds the existing row and, in `DELIVERY_MODE=naive`,
attempts to publish again on *that* call (see ADR 0003). If the retry
succeeds, the missing event is fixed by accident — a second, unrelated
request happened to run the exact code path that was skipped the first
time. If the client never retries (because it interpreted the failure as
a real, terminal rejection, which is indistinguishable from this case
from the client's point of view), the order stays permanently invisible
to every downstream consumer. **No amount of retry policy — more
attempts, longer backoff, jitter — changes the fact that the DB commit
and the Kafka publish are two separate, uncoordinated operations; a
retry only ever re-runs "step 2 after step 1," it can never go back and
make step 1 and step 2 atomic with each other.** The only fix is
structural: make the event's existence a *consequence* of the same
transaction that commits the business state, so there is no window where
one exists without the other. That is the transactional outbox (M03).

## Gap 2 — `orders.after_publish_before_response`

**What happens:** the Postgres transaction commits *and* the Kafka
publish succeeds — the broker has acknowledged the record. Before the
`202` response is built and sent, the process fails (or the
`FaultInjector` intercepts at this point).

**What the client sees:** an error response, exactly as indistinguishable
from Gap 1's failure as it was from a genuine rejection. The client has
no way to know the operation actually succeeded twice over (DB and
broker both durable).

**What's actually true, if the client retries (as a client reasonably
would, having seen only a failure):** the retry carries the same
`Idempotency-Key`. The DB layer correctly recognizes the replay and
never inserts a second order row. But the naive publish path (ADR 0003)
republishes on every accepted call, replay or not — it has no way to
know "we already published for this key" because nothing recorded that
fact anywhere the publish step can see. The result, reproduced
deterministically in `dual_write_publish_then_retry_duplicate`: **two
distinct events** (different `event_id`, same `order_id` /
`aggregate_id`) land on `orders.events.v1` for what the client
experienced as one logical request.

**Why retries cannot close this gap either — they're the direct cause of
it:** the retry is not a bug in the client; retrying an ambiguous failure
is the *correct* thing for a client to do, since "did it work?" is
genuinely unknowable from a failed response alone. The problem is that
the naive producer has no memory of what it already published, so a
retry that correctly avoids a duplicate *order* does nothing to avoid a
duplicate *event*. Making the client retry less eagerly doesn't help —
the ambiguity is inherent to "the broker ack happened before the
response was returned," not to how aggressively the client behaves.
Fixing this requires either (a) the producer deduplicating its own
publishes against a durable record of "have I published this logical
event already" (again: the outbox, which ties publication to a specific,
already-committed row rather than to "was this HTTP call accepted"), or
(b) pushing the burden downstream: every consumer of `orders.events.v1`
must treat delivery as at-least-once and be idempotent regardless
(spec invariant I11), which this project requires unconditionally from
M04 onward precisely because gaps like this one are expected to keep
happening even after M03 closes gap 1.

## Conclusion

Retries alone cannot make two independently-committing systems —
Postgres and Kafka — atomic with each other. A retry can only repeat an
operation; it cannot retroactively join two commits that already
happened (or failed to happen) independently, in either order, with an
arbitrary-length gap between them where the process can die. Gap 1 shows
a **retry can't invent a missing event** because nothing recorded that
one was owed. Gap 2 shows a **retry can actively create an extra event**
because nothing recorded that one was already sent. Both failure modes
are consequences of the same root cause: the business state change and
the event publication are not one atomic unit. The transactional outbox
(M03) fixes this by making the event's existence part of the same
database transaction as the business state change, so there is no
process-death window where one can happen without the other — and every
consumer built afterward (M04+) still has to handle at-least-once
delivery and duplicates on its own, because the outbox guarantees the
event is *never lost*, not that it is *never sent twice*.
