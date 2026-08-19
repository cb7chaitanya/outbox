# Operations runbook

## First checks

Use one correlation ID across JSON logs. Do not log request headers, tokens,
full payloads, idempotency keys, or provider references. Check:

```bash
curl -s localhost:8081/metrics
docker compose logs | jq 'select(.fields.correlation_id == "UUID")'
```

Prometheus queries: `outbox_oldest_unpublished_age_seconds`,
`consumer_lag_records`, `rate(dependency_retries_total[5m])`,
`increase(dlq_published_total[5m])`, and
`compensation_oldest_age_seconds`. Start the optional stack with
`docker compose --profile observability up -d`; Prometheus is on 9090,
Grafana on 3000, and Jaeger on 16686.

## Old outbox or broker outage

Confirm Redpanda health with `rpk cluster health`, then inspect
`outbox_unpublished_count`, age, failures, and lease recoveries. Restore the
broker and leave rows intact; publisher leases expire and retry automatically.
Never mark rows published manually.

## Consumer lag or poison record

Inspect `<source-topic>.dlq` in Redpanda Console. The record contains the
source topic/partition/offset/key, envelope when parseable, stable error code,
timestamps, and replay count. Correct the producer or data before replay.
Malformed envelopes without a parsed envelope are quarantined, not replayed.

Replay one corrected/valid record while preserving event identity:

```bash
cargo run -p replay-dlq -- inventory.commands.v1 12 localhost:19092
```

The CLI validates the source topic and adds `replayed_from_dlq` and
`replay_count` headers. Inbox and provider idempotency make repeated replay
effect-safe. Never clear a production business inbox; only disposable
projection state may be reset for a full projection rebuild.

## Long-running compensation

Query the order and transitions, then verify the reservation and payment in
their owning databases. A failed refund/release must produce `MANUAL_REVIEW`,
a `COMPENSATION_EXHAUSTED` DLQ record, and an error log. Do not force
`CANCELLED`; correct/replay the failed compensation or complete it through an
audited operator procedure.

## Database outage

Readiness returns 503 while liveness remains 200. Consumers do not advance
offsets before their local transaction commits. Restore Postgres, verify
`pg_isready`, then watch lag and outbox age converge. Do not wipe volumes.

## Chaos and fault controls

Run `make chaos-smoke`. Fault endpoints exist only when
`FAILURE_INJECTION_ENABLED=true`, require `X-Test-Token`, and configuration
refuses that setting when `ENVIRONMENT=production`. Every change is logged.
Use `DELETE /_test/faults` after an experiment. Never enable these controls in
a shared or production environment.
