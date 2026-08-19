# Event-Driven Order System

A production-shaped learning system for transactional outboxes, idempotent
consumers, ordering, replay, choreography, compensation, and recovery. It
retains a naive dual-write mode so its failures can be compared with the
correct design. Delivery is at least once with effectively-once business
effects—not a claim of end-to-end exactly once.

## Architecture

```text
Client -> Orders API/DB/outbox -> Redpanda
                                  /   |   \
                       Inventory  Payments  Fulfilment
                             \       |       /
                              outcome events
                                    |
                            Orders state machine
```

| Service | Owns | Port |
|---|---|---:|
| orders | lifecycle, idempotency, transitions, saga decisions | 8081 |
| inventory | stock and reservations | 8082 |
| payments | authorizations, refunds, provider ledger | 8083 |
| fulfilment | fulfilment creation outcome | 8084 |

Every service owns a separate Postgres database, inbox, outbox, migrations,
and offset ledger. There are no cross-service database joins.

## Five-minute quick start

Prerequisites: Docker Compose v2 and Rust via `rustup` (Rust 1.94 is pinned).

```bash
make setup
make up
```

`make up` starts infrastructure, creates topics, runs migrations, builds all
four services, and waits for readiness.

```bash
curl -X PUT localhost:8082/_dev/stock/SKU-1 \
  -H 'content-type: application/json' -d '{"available_qty":50}'

curl -i -X POST localhost:8081/v1/orders \
  -H 'content-type: application/json' -H 'idempotency-key: demo-001' \
  -H 'x-correlation-id: 018f0d56-9d45-7c01-a0aa-000000000001' \
  -d '{"items":[{"sku":"SKU-1","quantity":2,"unit_price_minor":1250}],"currency":"USD"}'

curl localhost:8081/v1/orders/ORDER_ID
curl localhost:8081/v1/orders/ORDER_ID/transitions
```

The same key/body replays the original response. The same key with another
body returns `409 IDEMPOTENCY_KEY_REUSED`.

## Workflow and compensation

```text
PENDING -> INVENTORY_RESERVED -> PAYMENT_AUTHORIZED
        -> READY_FOR_FULFILMENT -> COMPLETED
```

- Inventory rejection cancels without payment.
- Payment failure releases inventory before cancellation.
- Fulfilment failure refunds and releases; cancellation waits for both.
- Exhausted compensation emits a DLQ/operator signal and enters
  `MANUAL_REVIEW`.

Commands are explicit. Correlation, causation, and `traceparent` propagate.
Per-target command sequences prevent false cross-service version gaps.

## Dual-write lab and outbox recovery

```bash
make demo-naive-failure
```

This reproduces DB-commit-without-publish and ambiguous-retry duplicate
publication. Retries cannot atomically couple two systems; see
[the failure lab](docs/failure-lab.md).

Normal `DELIVERY_MODE=outbox` commits state and event together. Publishers use
short `FOR UPDATE SKIP LOCKED` leases. A crash after publish can republish, but
inbox identity/hash checks, versions, and provider idempotency make it harmless.

## Ordering, replay, and operations

Consumers apply, count stale, buffer recoverable fetched-window gaps, and DLQ
unresolved gaps as `EXPECTED_VERSION_GAP`. Offsets advance only through a
contiguous completed prefix.

```bash
cargo run -p replay-dlq -- inventory.commands.v1 DLQ_OFFSET localhost:19092
docker compose --profile observability up -d
```

Replay preserves the envelope's event/correlation identity and adds replay
headers. Metrics cover outbox age, lag, results, gaps, retries, DLQ, and
compensation age without ID/SKU/key labels. Prometheus is on 9090, Grafana on
3000, Jaeger on 16686, and Redpanda Console on 8090. See
[the runbook](docs/runbook.md).

## Verification

```bash
make test
make test-e2e
make chaos-smoke
make demo-naive-failure
```

Chaos covers broker/DB recovery, worker restart, poison isolation, correlated
logs, and metrics using bounded polling. Evidence is under [docs/evidence](docs/evidence).

## Configuration, lifecycle, and trade-offs

`.env.example` documents configuration. Failure injection is off by default,
token-protected, and refused in `production`.

```bash
make migrate
make down
make reset CONFIRM=yes   # destructive local volume reset
```

Postgres uses host port 55432 and Redpanda 19092. `rskafka` avoids native build
dependencies but lacks consumer groups, so the project uses a Postgres offset
ledger and one active consumer per partition. Topics are single-partition for
deterministic learning, not maximum throughput. Choreography is the required
default; the M11 orchestrator is optional after core completion.

Read [the learning write-up](docs/blog/project-2-event-driven-orders.md), [ADRs](docs/adr), and
[milestone evidence](docs/evidence).
