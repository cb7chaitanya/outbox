# ADR 0006: Local offset ledger in place of a Kafka consumer-group commit

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M04

## Context

Spec section 14 describes offset handling in terms of a broker-managed
consumer group: "commit the Kafka offset only after the database commit
succeeds," "disable Kafka auto-commit." That vocabulary assumes a
high-level consumer client with a group-coordinator protocol — the kind
`rdkafka`'s consumer offers.

ADR 0001 ruled `rdkafka` out for this sandbox (no cmake/libsasl2 build
toolchain) in favor of `rskafka`, a low-level, protocol-direct client.
`rskafka` has no consumer-group protocol at all: no group coordinator, no
partition assignment, no `commit()` call tied to a broker-side offset
store. `PartitionClient::fetch_records` only ever takes an explicit
starting offset the caller supplies.

## Decision

Inventory (and every future consumer) tracks "what's been read" itself, in
its own Postgres database, via a `consumer_offsets(consumer_name, topic,
partition, next_offset, updated_at)` table (`persistence::inbox::fetch_offset`
/ `commit_offset`). The eight-step handler protocol changes only in where
step 8 writes: instead of calling a broker's offset-commit API, it does an
`UPDATE`/`UPSERT` against this local ledger — and, critically, that write
still only happens *after* the record's business transaction (or DLQ
publish) has already committed, preserving the exact ordering guarantee
the spec cares about: never acknowledge a message before its effect is
durable.

## Alternatives considered

- **Re-fetch from the topic's earliest offset every restart, rely on the
  inbox table alone for dedup.** Correct, but wasteful and slow as topics
  grow, and it does nothing extra for correctness that the ledger doesn't
  also give — the inbox row is still the actual source of idempotency
  either way (see the crash-after-commit test, where redelivery is a
  no-op *because of the inbox row*, not because of the ledger). Rejected
  on efficiency grounds alone.
- **Switch to `rdkafka` for M04+ specifically, since it's the milestone
  that needs a real consumer.** Reopens the build-toolchain problem ADR
  0001 already ruled out, for a feature (broker-side group commit) this
  project doesn't actually need — every topic here has exactly one
  partition (spec section 8), so there's no partition-assignment problem
  a consumer group would solve.
- **Store the offset in-memory only.** Fails the entire point: a process
  restart would replay everything, and the "crash after DB commit / before
  offset commit" acceptance scenario has nothing to demonstrate without a
  durable ledger to leave stale.

## Consequences

- Offset tracking is per-service, in the same database as everything else
  that consumer writes — one fewer moving part (no separate coordinator
  dependency), and it can be read/written inside contexts where a
  Postgres transaction is already open if a future milestone wants that.
- This is *not* a broker-visible consumer group: tools like `rpk group
  describe` won't show lag for this consumer. Section 16's "consumer lag"
  metric (M09) will need to compute lag as `latest_offset(topic) -
  next_offset(consumer, topic, partition)` from this ledger plus
  [`Consumer::latest_offset`], rather than reading it off the broker
  directly.
- Multiple replicas of the same consumer service are not yet safe to run
  concurrently against the same topic/partition with this design — there's
  no equivalent of consumer-group partition assignment preventing two
  replicas from both reading and racing to process the same offset range.
  Single-instance-per-consumer is an implicit assumption through M04;
  revisit if a later milestone needs horizontal consumer scaling.
