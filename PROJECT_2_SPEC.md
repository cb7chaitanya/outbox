# Project 2 Specification: Event-Driven Order System

**Status:** implementation contract  
**Audience:** Claude Code or another autonomous coding agent  
**Primary stack:** Rust, Tokio, Axum, SQLx, PostgreSQL, Redpanda (Kafka-compatible)  
**Purpose:** learn distributed consistency by first building a deliberately unsafe dual-write system, reproducing its failures, then evolving it through transactional outbox, idempotent consumers, choreography, compensation, and optional orchestration.

---

## 1. Agent operating contract

This file is the source of truth. At the start of every work session:

1. Read this file completely.
2. Inspect the repository, `git status`, milestone evidence, migrations, tests, and README.
3. Find the first milestone whose acceptance checklist is not fully satisfied.
4. Implement only that milestone and prerequisites explicitly listed for it.
5. Run formatting, linting, unit tests, integration tests, and the milestone acceptance commands.
6. Fix failures rather than weakening, deleting, ignoring, or marking tests flaky.
7. Update the milestone evidence file and documentation with exact commands and observed results.
8. Commit the milestone as specified in the commit plan.
9. Re-read this file and repeat from step 2 until all required milestones are complete.

### Non-negotiable progression rule

**Do not skip ahead.** The naive dual-write implementation must exist, its two inconsistency windows must be reproduced deterministically, and the evidence must be committed before the transactional outbox is introduced. Choreography must be complete and tested before the optional orchestrator is implemented. Do not silently replace an educational intermediate design with the final design.

An agent may refactor already-completed code only when required by the current milestone. It must preserve earlier failure demonstrations as runnable historical tests or harness modes. A milestone is complete only when every acceptance item is backed by code, tests, or documentation. Checkboxes are evidence summaries, not substitutes for evidence.

### Required progress files

- `docs/progress.md`: checklist of milestones, current milestone, decisions, commands run, results, and next action.
- `docs/adr/`: architecture decision records.
- `docs/evidence/mNN.md`: acceptance evidence for milestone NN, including test names and relevant logs.
- Never claim a command passed unless it was actually run in the current repository state.
- If blocked by unavailable Docker or infrastructure, finish safe code work, record the exact blocker, and leave the milestone incomplete.

---

## 2. Product narrative

A client submits an order containing one or more SKU quantities and an amount. The system creates the order, reserves inventory, authorizes payment, and arranges fulfilment through asynchronous events. Failures trigger compensation so stock and money are not stranded. The read API exposes the current order state and a transition history.

This is a learning system, not a storefront. It emphasizes delivery semantics, atomicity boundaries, idempotency, ordering, replay, concurrency, failure recovery, and operational evidence.

## 3. Goals

- Demonstrate why database-plus-broker dual writes are unsafe.
- Implement atomic local state change plus transactional outbox.
- Provide at-least-once publication and consumption with duplicate safety.
- Use event choreography for the primary workflow.
- Implement compensating actions for partial success.
- Enforce aggregate versions and valid state transitions under concurrency.
- Make retries bounded, classified, observable, and dead-lettered.
- Preserve correlation and causation across the workflow.
- Support deterministic failure injection and invariant testing.
- Support replay without duplicating business effects.
- Run locally using a single documented Docker Compose workflow.
- Produce interview-quality documentation explaining trade-offs and failure modes.

## 4. Non-goals

- Exactly-once delivery as a broker guarantee.
- Production PCI handling or real payment-provider integration.
- Authentication, authorization, customer accounts, tax, discounts, shipping rates, or UI.
- Global ACID transactions or two-phase commit.
- Multi-region deployment, Kubernetes, service mesh, or cloud provisioning.
- Schema Registry as a required dependency; JSON envelopes are sufficient.
- High-throughput benchmarking before correctness milestones are complete.
- Event sourcing: service tables remain the source of local business state.

## 5. Core invariants

Use these as named assertions in tests and metrics where applicable.

1. **I1 — Order identity:** one logical order exists per accepted idempotency key.
2. **I2 — Legal transitions:** an order state changes only through the transition graph in section 12.
3. **I3 — Local atomicity:** a committed business state change that requires an event has exactly one corresponding outbox row in the same database transaction.
4. **I4 — No phantom event:** no outbox event exists for a rolled-back business change.
5. **I5 — Inventory conservation:** `available + reserved = initial - fulfilled` for each SKU; neither available nor reserved is negative.
6. **I6 — Single reservation effect:** duplicate reservation requests do not reserve stock twice.
7. **I7 — Single payment effect:** a payment is authorized/captured/refunded at most once per logical operation.
8. **I8 — Fulfilment precondition:** fulfilment is created only after both inventory reservation and payment authorization for the same order.
9. **I9 — Terminal exclusivity:** an order cannot be both completed and cancelled.
10. **I10 — Compensation convergence:** a cancelled order eventually has no active inventory reservation and no unrefunded authorization.
11. **I11 — Inbox uniqueness:** a consumer applies each `(consumer_name, event_id)` at most once.
12. **I12 — Version monotonicity:** aggregate versions increase by one per accepted mutation and never decrease.
13. **I13 — Stale-event safety:** an older or already-applied aggregate version cannot regress state.
14. **I14 — Traceability:** every domain event has event, correlation, causation, aggregate, schema-version, producer, and occurrence metadata.
15. **I15 — Poison isolation:** a permanently invalid event cannot block its partition forever; it reaches a DLQ with failure metadata.

“Exactly once” in this project always means **effectively-once business effects implemented above at-least-once transport**, never a claim of end-to-end exactly-once delivery.

---

## 6. Architecture

```text
Client
  |
  v
Orders API ---- PostgreSQL (orders + transitions + outbox + inbox)
  | outbox publisher
  v
Redpanda topics
  |--------------------|--------------------|
  v                    v                    v
Inventory          Payments             Fulfilment
PostgreSQL         PostgreSQL            PostgreSQL
 + outbox/inbox     + outbox/inbox        + outbox/inbox
  |                    |                    |
  +--------------------+--------------------+
                       events
                         |
                         v
                     Orders projection/state machine

Optional final extension: Saga Orchestrator consumes workflow events and emits
commands. It is not introduced until choreography acceptance is complete.
```

Each service owns its tables. For local development they may share one PostgreSQL server, but must use separate databases or schemas, separate migration directories, and separate SQLx pools. No service reads or writes another service's tables. Cross-service joins are forbidden.

### Service boundaries

| Service | Owns | Synchronous API | Consumes | Produces |
|---|---|---|---|---|
| orders | order lifecycle, client idempotency, transition history | create/get order, health | inventory/payment/fulfilment outcomes | `OrderCreated`, cancellation/compensation requests, terminal outcomes |
| inventory | stock and reservations | health; dev-only stock seed/read | order/reservation/release commands | reservation succeeded/failed, stock released |
| payments | payment attempts and refunds | health | authorization/refund commands | payment authorized/failed/refunded |
| fulfilment | shipment/fulfilment creation | health | readiness/create/cancel commands | fulfilment created/failed/cancelled |
| saga-orchestrator (optional) | saga instance and step state only | health, debug saga read | all relevant outcomes | explicit commands for the next step/compensation |

Choreography may use events that semantically request work, but name them explicitly as commands when there is a single intended handler. Events are facts in past tense; commands are imperative. Do not mix the two in one type.

---

## 7. Repository/workspace structure

```text
.
├── Cargo.toml                  # workspace, shared dependency versions
├── Cargo.lock
├── rust-toolchain.toml
├── .env.example
├── .gitignore
├── Makefile                    # thin, documented developer commands
├── docker-compose.yml
├── crates/
│   ├── contracts/              # event envelope, payloads, validation
│   ├── messaging/              # producer/consumer abstractions, Kafka adapter
│   ├── persistence/            # reusable outbox/inbox primitives, not domain repos
│   ├── telemetry/              # tracing, metrics, propagation
│   ├── test-support/           # containers/fixtures/fault controls/eventually
│   └── domain-common/          # IDs, money, error taxonomy, clocks
├── services/
│   ├── orders/
│   ├── inventory/
│   ├── payments/
│   ├── fulfilment/
│   └── saga-orchestrator/      # absent until optional milestone starts
│       ├── Cargo.toml
│       ├── src/{main,config,http,domain,application,adapters}.rs
│       ├── migrations/
│       └── tests/
├── migrations/README.md
├── tests/
│   ├── e2e/
│   ├── invariants/
│   └── fixtures/
├── scripts/                    # safe setup, topic creation, demos
├── docs/
│   ├── progress.md
│   ├── architecture.md
│   ├── failure-lab.md
│   ├── runbook.md
│   ├── adr/
│   ├── evidence/
│   └── blog/
└── README.md
```

Use edition 2024 if supported by the pinned stable toolchain; otherwise edition 2021 and document why. Pin dependency versions at workspace level. Use `thiserror` for typed library/domain errors and `anyhow` only at process boundaries. Inject `Clock`, ID generation, fault injection, and messaging ports to keep tests deterministic.

---

## 8. Event transport contract

### Topics

- `orders.events.v1`
- `inventory.commands.v1`, `inventory.events.v1`
- `payments.commands.v1`, `payments.events.v1`
- `fulfilment.commands.v1`, `fulfilment.events.v1`
- `saga.commands.v1` only if the orchestrator milestone is enabled
- `<source-topic>.dlq` for each consumed source topic

All order-workflow records use `order_id` as the Kafka message key. This preserves per-order partition ordering; no global ordering is assumed. Topic creation is explicit and auto-topic creation is disabled in Compose.

### Canonical JSON envelope

```json
{
  "event_id": "018f...uuid-v7",
  "event_type": "inventory.reservation_succeeded",
  "schema_version": 1,
  "occurred_at": "2026-01-01T00:00:00Z",
  "producer": "inventory",
  "aggregate_type": "inventory_reservation",
  "aggregate_id": "order-uuid",
  "aggregate_version": 2,
  "correlation_id": "order-uuid-or-request-uuid",
  "causation_id": "event-or-command-uuid",
  "traceparent": "optional W3C trace context",
  "payload": {}
}
```

Required validation: UUID formats, known type/version pair, UTC timestamp, non-empty producer and aggregate fields, version greater than zero, payload validation, and configured maximum message size. Unknown fields are tolerated. Unknown schema versions are non-retryable and go to DLQ. Never put secrets, raw card data, or unrestricted PII in events.

### Domain events and commands

| Type | Minimum payload |
|---|---|
| `orders.order_created` | `order_id`, `items[{sku,quantity}]`, `amount{currency,minor_units}` |
| `inventory.reserve_inventory` | `order_id`, `items`, `expected_order_version` |
| `inventory.reservation_succeeded` | `order_id`, `reservation_id`, `items` |
| `inventory.reservation_failed` | `order_id`, `reason_code`, `items` |
| `inventory.release_inventory` | `order_id`, `reservation_id`, `reason` |
| `inventory.inventory_released` | `order_id`, `reservation_id` |
| `payments.authorize_payment` | `order_id`, `payment_id`, `amount` |
| `payments.payment_authorized` | `order_id`, `payment_id`, `provider_reference` (fake) |
| `payments.payment_failed` | `order_id`, `payment_id`, `reason_code` |
| `payments.refund_payment` | `order_id`, `payment_id`, `reason` |
| `payments.payment_refunded` | `order_id`, `payment_id` |
| `fulfilment.create_fulfilment` | `order_id`, `reservation_id`, `payment_id` |
| `fulfilment.fulfilment_created` | `order_id`, `fulfilment_id` |
| `fulfilment.fulfilment_failed` | `order_id`, `reason_code` |
| `orders.order_completed` | `order_id`, `fulfilment_id` |
| `orders.order_cancelled` | `order_id`, `reason_code` |

Payload structs are defined once in `contracts`; producers and consumers use the same serialized fixtures. Store the complete envelope in outbox/inbox/DLQ records. Compatibility policy within v1: add optional fields only; breaking changes require a new event schema version and compatibility tests.

---

## 9. Per-service data models

All tables use `timestamptz`, UUID identifiers, explicit constraints, and useful indexes. Monetary values are integer minor units plus ISO currency; floating point is forbidden.

### Shared infrastructure tables in every consuming/producing service

```sql
outbox_events(
  id uuid primary key,
  aggregate_type text not null,
  aggregate_id uuid not null,
  aggregate_version bigint not null,
  topic text not null,
  message_key text not null,
  envelope jsonb not null,
  created_at timestamptz not null,
  published_at timestamptz null,
  attempts int not null default 0,
  next_attempt_at timestamptz not null,
  last_error text null,
  claimed_by text null,
  claimed_until timestamptz null,
  unique(aggregate_type, aggregate_id, aggregate_version, topic)
);

inbox_events(
  consumer_name text not null,
  event_id uuid not null,
  source_topic text not null,
  source_partition int not null,
  source_offset bigint not null,
  aggregate_id uuid not null,
  aggregate_version bigint not null,
  received_at timestamptz not null,
  processed_at timestamptz null,
  payload_hash text not null,
  primary key(consumer_name, event_id)
);

consumer_aggregate_versions(
  consumer_name text not null,
  aggregate_id uuid not null,
  last_version bigint not null,
  primary key(consumer_name, aggregate_id)
);
```

### Orders

- `orders(id, idempotency_key unique, status, currency, amount_minor, version, cancellation_reason, created_at, updated_at)`
- `order_items(order_id, sku, quantity, unit_price_minor, primary key(order_id, sku))`
- `order_transitions(id, order_id, from_status, to_status, reason, triggering_event_id, order_version, created_at, unique(order_id, order_version))`
- Status: `PENDING`, `INVENTORY_RESERVED`, `PAYMENT_AUTHORIZED`, `READY_FOR_FULFILMENT`, `COMPLETED`, `CANCELLING`, `CANCELLED`, `MANUAL_REVIEW`.

### Inventory

- `stock(sku primary key, available_qty check >= 0, reserved_qty check >= 0, fulfilled_qty check >= 0, version)`
- `reservations(id, order_id unique, status, version, created_at, updated_at)`
- `reservation_items(reservation_id, sku, quantity check > 0, primary key(reservation_id, sku))`
- Reservation status: `ACTIVE`, `RELEASED`, `COMMITTED`, `REJECTED`.
- Reserve all SKUs in one local transaction, locking stock rows in sorted SKU order to avoid deadlock.

### Payments

- `payments(id, order_id unique, currency, amount_minor, status, provider_reference unique null, version, failure_code, created_at, updated_at)`
- `payment_operations(id, payment_id, operation_type, idempotency_key unique, status, attempts, created_at, updated_at)`
- Status: `PENDING`, `AUTHORIZED`, `FAILED`, `REFUND_PENDING`, `REFUNDED`.
- The fake provider must itself honor operation idempotency keys.

### Fulfilment

- `fulfilments(id, order_id unique, reservation_id, payment_id, status, version, failure_code, created_at, updated_at)`
- Status: `PENDING`, `CREATED`, `FAILED`, `CANCELLED`.

### Optional orchestrator

- `sagas(id, order_id unique, state, version, failure_reason, created_at, updated_at)`
- `saga_steps(id, saga_id, step_name, status, command_event_id unique, outcome_event_id unique null, attempts, created_at, updated_at)`

Every mutation uses `UPDATE ... WHERE id = $1 AND version = $expected`, increments the version, and checks exactly one affected row. A zero-row update is a typed `VersionConflict`, not an implicit success.

---

## 10. HTTP API contracts

JSON only. Return `application/problem+json` errors with `type`, `title`, `status`, `code`, `detail`, `request_id`, and field violations when relevant.

### Orders API

`POST /v1/orders`

Headers:

- `Idempotency-Key` required, 8–128 printable characters.
- `X-Correlation-ID` optional UUID; generate one if absent.

Request:

```json
{
  "items": [{"sku": "SKU-1", "quantity": 2, "unit_price_minor": 1250}],
  "currency": "USD"
}
```

Rules: 1–100 distinct items; positive quantities with a configured maximum; uppercase currency; checked integer arithmetic; amount computed server-side. Response `202 Accepted` with order representation and `Location: /v1/orders/{id}`. Same idempotency key plus byte-equivalent normalized request returns the original response. Same key with different request returns `409 IDEMPOTENCY_KEY_REUSED`.

`GET /v1/orders/{order_id}` returns the order, items, version, current status, timestamps, and links. `GET /v1/orders/{order_id}/transitions` returns ordered transition history. `POST /v1/orders/{order_id}/cancel` is an optional client action only after core flow completion and must be idempotent.

Common status codes: `400` validation, `404`, `409` conflict, `422` semantic error, `429` overload, `500`, `503` dependency unavailable.

### Operational endpoints on every service

- `GET /health/live`: process alive; never checks dependencies.
- `GET /health/ready`: checks required DB and broker connectivity with bounded timeout.
- `GET /metrics`: Prometheus text format.
- Dev/test-only fault endpoints described in section 17, disabled unless `FAILURE_INJECTION_ENABLED=true`.

---

## 11. Deliberately naive dual-write stage

This stage is mandatory and must remain reproducible after later milestones.

Initial orders flow:

1. Insert order and commit PostgreSQL transaction.
2. Publish `orders.order_created` directly to Kafka.
3. Return `202` only if both appear successful.

Implement two named fault points:

- `orders.after_db_commit_before_publish`: terminate or return injected failure after commit. Result: order exists but event does not.
- `orders.after_publish_before_response`: terminate or return injected failure after broker acknowledgement. A client retry can create or publish a duplicate unless idempotency protects creation; consumers still see redelivery/duplicates.

Required deterministic demonstrations:

- `dual_write_db_commit_without_event`: assert the row exists and no matching event arrives within a bounded interval.
- `dual_write_publish_then_retry_duplicate`: assert at least two deliveries or demonstrate why producer/client retry makes outcome ambiguous; record event IDs and business key.
- A script `scripts/demo-dual-write-failure.sh` runs both cases and prints invariant violations.
- `docs/failure-lab.md` explains the two atomicity gaps and why retries alone cannot close them.

Do not add the outbox until these tests pass as demonstrations of failure. After outbox introduction, retain the naive adapter behind `DELIVERY_MODE=naive|outbox`, defaulting to `outbox`, so the lab remains runnable.

---

## 12. Workflow and state transitions

### Choreography-first happy path

1. Orders commits `PENDING` + `OrderCreated` outbox event.
2. Inventory consumes it, reserves stock, emits `ReservationSucceeded` or `ReservationFailed`.
3. Payment authorization begins only after reservation succeeds.
4. Orders records relevant outcomes. When reservation and authorization are both true, fulfilment is requested.
5. Fulfilment emits created or failed.
6. Orders transitions to `COMPLETED` on fulfilment success.

Allowed order transitions:

```text
PENDING -> INVENTORY_RESERVED -> PAYMENT_AUTHORIZED -> READY_FOR_FULFILMENT -> COMPLETED
PENDING | INVENTORY_RESERVED | PAYMENT_AUTHORIZED | READY_FOR_FULFILMENT -> CANCELLING
CANCELLING -> CANCELLED | MANUAL_REVIEW
```

If events can arrive in a different order, store independent facts (reservation/payment IDs and outcome flags) and derive readiness; do not force an invalid transition merely because payment arrived before an orders projection update. Either buffer a future version or safely re-evaluate after each fact.

### Compensation matrix

| Failure | Required actions | Final order state |
|---|---|---|
| inventory rejected | no payment attempted; cancel | `CANCELLED` |
| payment failed after reservation | release inventory; cancel after release confirmation | `CANCELLED` |
| fulfilment failed after reservation + authorization | refund payment and release inventory; cancel after both confirmations | `CANCELLED` |
| compensation transient failure | bounded retry | `CANCELLING` |
| compensation retry budget exhausted/permanent failure | DLQ + operator signal | `MANUAL_REVIEW` |

Compensations are idempotent. Releasing an already released reservation and refunding an already refunded payment return logical success without repeating the external effect. Never “undo” by deleting audit rows.

### Optional saga orchestrator

After choreography is fully accepted, add an alternate `WORKFLOW_MODE=choreography|orchestrated`. The orchestrator owns workflow decisions and emits one command at a time. Services remain unaware of saga policy. It uses inbox/outbox and optimistic concurrency like every other service. The same end-to-end invariant suite must pass in both modes. Document coupling, visibility, failure recovery, and operational trade-offs in an ADR. This milestone is optional for core completion but required for “Project 2+ orchestration complete.”

---

## 13. Transactional outbox and publisher semantics

Insert the business mutation and serialized outbox envelope using the same SQLx transaction and the same database connection. Roll back both on any error. Event IDs are generated once before transaction retry and remain stable for the logical mutation.

Publisher workers:

1. Select eligible unpublished rows ordered by `created_at, id` using `FOR UPDATE SKIP LOCKED` in small batches.
2. Claim rows with a lease and commit the claim quickly; do not hold a DB transaction open during network I/O.
3. Publish with topic, key, envelope, `event_id` header, correlation/causation headers, and producer name.
4. Require broker acknowledgement (`acks=all` in local configuration).
5. On acknowledgement, mark `published_at`; on error, increment attempts, save sanitized error, and compute `next_attempt_at`.
6. If a worker dies after publish and before marking published, the row is published again after lease expiry. Consumers must therefore be idempotent.

The publisher guarantees at-least-once attempts, not exactly once. Multiple service replicas may publish concurrently without publishing an actively claimed row. Add metrics for backlog count, oldest unpublished age, publish attempts, failures, and lease recoveries. Retain published rows for the project; a documented maintenance command may archive them later.

Ordering rule: the key preserves broker partition order, while the aggregate version provides semantic order. The publisher should normally emit an aggregate's versions in order but consumers must still handle duplicates and stale events. A failed earlier version must prevent a later version for the same aggregate from causing illegal state; other aggregates must continue.

---

## 14. Idempotent consumer and inbox semantics

For every handler, one local transaction must:

1. Validate the envelope and payload.
2. Insert inbox identity with `ON CONFLICT DO NOTHING`.
3. If already present, verify stored payload hash matches; acknowledge without business work. A hash mismatch for the same event ID is a security/data-integrity error sent to DLQ and alerted.
4. Check aggregate version/order policy.
5. Apply the business mutation with optimistic concurrency.
6. Insert any resulting outbox events.
7. Mark inbox processed and commit.
8. Commit the Kafka offset only after the database commit succeeds.

If the process dies between DB commit and offset commit, redelivery becomes a no-op because of the inbox row. Never commit an offset before local commit. Disable Kafka auto-commit.

### Ordering and replay policy

- `incoming_version == last_version + 1`: apply.
- `incoming_version <= last_version`: acknowledge as stale/duplicate and record metric.
- `incoming_version > last_version + 1`: classify as a gap; retry/buffer for a bounded interval without blocking unrelated keys, then DLQ with `EXPECTED_VERSION_GAP` if unresolved.
- Outcome events from different aggregates must not be compared using one shared version sequence. Orders records each source fact using its event ID and source aggregate version.
- A replay uses a new consumer group or clears only a disposable projection/inbox specifically documented for replay. Replaying against business-effect consumers must remain safe and must not contact the fake provider twice.
- Provide an admin CLI to replay a DLQ record after correction; the replay preserves original event and correlation IDs and adds replay metadata.

---

## 15. Retry, backoff, budgets, and DLQ

Centralize error classification:

| Class | Examples | Action |
|---|---|---|
| transient | timeout, connection reset, broker unavailable, SQL serialization/deadlock | retry |
| contention | optimistic version conflict that may resolve, row lock | short retry/re-read |
| rate limited | provider `429`/overload | honor retry-after, retry |
| permanent | invalid payload, unsupported schema, impossible transition, insufficient stock, declined payment | business failure event or DLQ as appropriate; no blind retry |
| poison/integrity | same event ID different hash, corrupt JSON | immediate DLQ + alert |

Use exponential backoff with full jitter: random delay in `[0, min(cap, base * 2^attempt)]`. Defaults: base 100 ms, cap 30 s, max 8 attempts, max elapsed 10 minutes. Make values configurable and use a seeded RNG/fake clock in tests. A retry budget is enforced per message and also protected by a per-service concurrency limit so one dependency cannot create an unbounded retry storm.

DLQ records contain original topic/partition/offset/key/envelope, consumer, attempt count, first/last failure time, stable error code, sanitized message, stack/source summary, and replay count. Publishing to DLQ must be acknowledged before the poison message offset is committed. If DLQ publication fails, do not lose the source record; retry DLQ publication with a distinct bounded operational alert. Metrics and structured logs expose DLQ count. `docs/runbook.md` documents inspect, correct, replay, and quarantine actions.

Business rejection (e.g. insufficient inventory or declined card) normally produces a domain failure event, not a DLQ. DLQ is for messages the consumer cannot correctly process.

---

## 16. Correlation, causation, and observability

- On order creation, use supplied correlation UUID or generate one; persist it.
- A resulting event inherits correlation ID and sets causation ID to the triggering request/event ID.
- Each log line includes service, environment, request/event ID, correlation ID, causation ID, aggregate ID/version, topic/partition/offset when relevant, attempt, and error code.
- Use `tracing` with JSON output. Redact headers/secrets and avoid payload logging by default.
- Propagate W3C `traceparent` through HTTP and Kafka headers/envelope.
- OpenTelemetry traces cover HTTP request, DB transaction, outbox claim/publish, consume, handler transaction, and fake provider call.
- Prometheus metrics include HTTP latency/errors, consumer lag, handler duration/result, retries by code, duplicate/stale/gap counts, outbox backlog/age, DLQ count, state-transition count, and compensation age.
- Avoid unbounded metric labels: no order ID, event ID, raw error, SKU, or idempotency key labels.

Provide a Compose observability profile with Prometheus and Grafana; Jaeger or Tempo is recommended. Include a dashboard or documented queries for one order's correlated journey and alerts for old outbox rows, growing lag, DLQ records, and long-running compensation.

---

## 17. Failure injection and chaos controls

Failure injection is disabled by default and must refuse to enable in an environment named `production`. Controls support a deterministic count (“fail next N”), stable fault name, optional order/event filter, and seeded delay.

Required fault points:

- after business DB commit/before direct publish (naive only)
- after broker publish/before outbox mark-published
- after consumer DB commit/before offset commit
- before/after inventory row locking
- payment provider timeout, decline, and success-response loss
- fulfilment permanent/transient failure
- malformed/unsupported envelope injection
- DB unavailable and broker unavailable via Compose stop/pause instructions
- graceful and forced publisher/consumer termination

Expose dev/test-only `PUT /_test/faults/{name}` and `DELETE /_test/faults`, or an equivalent control socket. Require a test token, bind locally, log every configuration change, and compile or runtime-disable it outside dev/test. Never implement faults as arbitrary sleeps scattered through domain code; use an injected `FaultInjector` port.

---

## 18. Testing strategy

### Layers

- Unit: state machines, money arithmetic, validation, error classification, backoff bounds, envelope compatibility, compensation decisions.
- Repository: real PostgreSQL migrations, constraints, optimistic updates, outbox atomicity, inbox uniqueness, row-lock behavior.
- Messaging integration: real Redpanda, topic keys, headers, redelivery, offset behavior, DLQ.
- Service integration: each handler with real DB/broker and deterministic fake provider.
- End-to-end: Compose system through public Orders API.
- Invariant/property tests: generated duplicates, reorderings, retries, and failures using seeded randomness (`proptest` acceptable).

### Deterministic invariant harness

Create a model-based test harness with a fake clock and seeded IDs/RNG. It records accepted commands/events and polls only through a reusable bounded `eventually` helper. Fixed sleeps are forbidden in correctness assertions. On failure print seed, event history, database snapshots, correlation ID, and broker offsets so the run is reproducible.

Required scenarios:

1. happy path converges to completed.
2. insufficient inventory cancels without payment.
3. payment decline releases inventory.
4. fulfilment failure refunds and releases.
5. duplicate every event 2–5 times; effects remain singular.
6. crash after consume DB commit; redelivery is no-op.
7. crash after publish before outbox update; duplicate is harmless.
8. concurrent same idempotency key creates one order.
9. concurrent inventory requests never oversell.
10. stale event cannot regress status.
11. version gap is recovered or DLQed predictably.
12. broker outage accumulates outbox, then drains after recovery.
13. DB outage does not acknowledge uncommitted messages.
14. poison event reaches DLQ and following records/other keys progress.
15. replay does not duplicate payment/reservation/fulfilment effects.
16. compensation retry exhaustion produces manual review and alert metric.

Tests must be race-safe and runnable repeatedly. Any unavoidable timing bound is generous, centralized, and documented.

---

## 19. Docker Compose and local development

Compose must provide PostgreSQL, Redpanda, Redpanda Console, all implemented services, migration jobs, and optional observability profile. Use health checks and dependency readiness, not startup sleeps. Persist data in named volumes, use a single-node development broker, expose only documented ports, and use non-secret local credentials mirrored in `.env.example`.

Required developer commands (Make targets may wrap equivalent commands):

```text
make setup          # validate tools, copy/describe env, build images
make up             # infrastructure + services, wait for readiness
make down           # stop without deleting data
make reset          # explicit destructive local-only data reset with confirmation or flag
make migrate
make topics
make fmt
make lint           # cargo clippy --workspace --all-targets --all-features -- -D warnings
make test-unit
make test-integration
make test-e2e
make test           # full required suite
make demo-naive-failure
make chaos-smoke
make logs ORDER_ID=<uuid>
```

Pin image versions. Add resource limits reasonable for a laptop. The README must include prerequisites, ports, troubleshooting, clean shutdown, data reset warning, and curl examples.

---

## 20. Milestones and acceptance gates

Do these strictly in order. Create `docs/evidence/mNN.md` only after running that milestone's commands.

### M00 — Repository contract and skeleton

Deliver workspace, toolchain pin, formatting/lint config, service skeletons, shared crates, Compose infrastructure, `.env.example`, progress file, and ADR template.

Acceptance:

- [ ] `cargo fmt --all -- --check`, workspace build, and clippy pass.
- [ ] PostgreSQL and Redpanda become healthy; topics are created explicitly.
- [ ] Each skeleton service exposes live/ready endpoints.
- [ ] README quick start works from a clean checkout.
- [ ] No orchestrator code exists yet.

Commit: `chore: scaffold project-2 workspace and local infrastructure`

### M01 — Orders API and local consistency

Implement orders migrations, domain state machine, create/get/transitions API, request validation, client idempotency, optimistic versioning, structured errors, and tests. No broker publish yet except a stub/port.

Acceptance:

- [ ] Concurrent identical idempotent requests yield one order and same response.
- [ ] Reused key with different request yields 409.
- [ ] Overflow and invalid item tests pass.
- [ ] Illegal and stale transitions fail without partial writes.

Commit: `feat(orders): add idempotent order API and versioned state machine`

### M02 — Naive dual write and failure lab

Add direct Kafka publish after DB commit, event contracts, deterministic fault points, demo script, and failure documentation. Do not add outbox tables yet.

Acceptance:

- [ ] Normal `OrderCreated` publication works with order ID as key.
- [ ] Both required dual-write failure tests reproduce and explain inconsistency.
- [ ] Evidence includes correlation IDs, DB state, and observed Kafka records.
- [ ] The failure-lab document explicitly concludes retries do not make two systems atomic.

Commit: `feat(learning): reproduce database-kafka dual-write failures`

### M03 — Transactional outbox

Add outbox migration/repository, atomically create order + event, publisher with claim leases, bounded retries, and naive/outbox delivery modes.

Acceptance:

- [ ] Transaction rollback produces neither order nor outbox event.
- [ ] Committed order always has exactly one logical outbox row.
- [ ] Crash after publish/before mark causes duplicate delivery and eventual published mark.
- [ ] Two publishers safely share backlog using skip-locked claims.
- [ ] Broker outage grows backlog; recovery drains it.
- [ ] Naive failure demo remains runnable; outbox mode closes its lost-event window.

Commit: `feat(messaging): replace dual write with transactional outbox`

### M04 — Inventory consumer and idempotent inbox

Implement stock/reservation model, inbox transaction, offset discipline, duplicates, locking, optimistic versions, inventory events, and DLQ baseline.

Acceptance:

- [ ] Duplicate `OrderCreated`/reserve work creates one reservation.
- [ ] Concurrent orders cannot oversell and stock invariant holds.
- [ ] Multi-SKU reservation is all-or-nothing.
- [ ] Crash after DB commit/before offset commit produces no duplicate effect.
- [ ] Invalid schema reaches DLQ without blocking valid work.

Commit: `feat(inventory): add idempotent reservation consumer and inbox`

### M05 — Payments with retry taxonomy

Implement fake provider, authorization, provider idempotency, error classes, full-jitter retry, retry budgets, events, refund operation, metrics, and deterministic fault tests.

Acceptance:

- [ ] Timeout then success authorizes once.
- [ ] Lost success response followed by retry creates one provider operation.
- [ ] Decline emits business failure without DLQ retry storm.
- [ ] Poison input reaches DLQ; retry metrics/error codes are correct.
- [ ] Refund is idempotent.

Commit: `feat(payments): add idempotent authorization and bounded retries`

### M06 — Choreographed workflow and compensation

Wire Orders, Inventory, and Payments through choreography. Orders stores outcome facts and drives legal state. Implement inventory release and cancellation convergence.

Acceptance:

- [ ] Happy path reaches payment-authorized/readiness without synchronous cross-service calls.
- [ ] Inventory failure cancels with no payment operation.
- [ ] Payment failure releases inventory then cancels.
- [ ] Duplicated and reordered outcomes do not create illegal transitions.
- [ ] Correlation/causation chain is complete.

Commit: `feat(workflow): implement choreographed order and compensation flow`

### M07 — Fulfilment and complete compensation matrix

Implement fulfilment service, readiness command/event flow, completed state, fulfilment failure compensation, manual review after exhausted compensation.

Acceptance:

- [ ] Happy path ends `COMPLETED` with one fulfilment.
- [ ] Fulfilment failure causes one refund and one inventory release.
- [ ] Order becomes `CANCELLED` only after required compensation confirmations.
- [ ] Exhausted compensation becomes `MANUAL_REVIEW` with DLQ/runbook signal.
- [ ] Terminal exclusivity and all invariants hold under duplicates.

Commit: `feat(fulfilment): complete order saga and compensations`

### M08 — Ordering, replay, and concurrency hardening

Implement aggregate version tracking, stale/gap policy, replay CLI, concurrency and property tests, payload-hash integrity checks, and documented partition assumptions.

Acceptance:

- [ ] Stale events are harmless and counted.
- [ ] Gaps recover within budget or go to DLQ with expected code.
- [ ] Replay is effect-safe and preserves event identity/metadata.
- [ ] Seeded duplicate/reordering property suite passes repeatedly.
- [ ] SQLx concurrency tests prove zero-row optimistic update handling.

Commit: `feat(consistency): harden ordering replay and optimistic concurrency`

### M09 — Observability, chaos, and operations

Complete traces, metrics, dashboards/queries, fault controls, chaos smoke test, alerts, and runbook.

Acceptance:

- [ ] One correlation ID reconstructs an order across all services.
- [ ] Outbox age, lag, retries, duplicates, DLQ, and compensation age are visible.
- [ ] Fault controls are disabled by default and impossible in production config.
- [ ] Broker/DB outage and worker-kill chaos scenarios recover without invariant breach.
- [ ] Logs and DLQ redact secrets and avoid high-cardinality metric labels.

Commit: `feat(ops): add end-to-end telemetry and deterministic chaos controls`

### M10 — Final acceptance, README, and learning write-up

Run the final suite from a clean environment, tighten documentation, add diagrams and interview notes, and resolve all warnings/TODOs in required scope.

Acceptance:

- [ ] All section 21 gates pass twice from a reset environment.
- [ ] README includes architecture, quick start, workflow, demos, failure recovery, and trade-offs.
- [ ] Blog draft tells the progression from dual-write failure to effectively-once effects.
- [ ] No undocumented required manual step, ignored test, placeholder, or unowned TODO remains.
- [ ] `docs/progress.md` marks required milestones complete with evidence links.

Commit: `docs: finalize project-2 acceptance evidence and learning guide`

### M11 — Optional saga orchestrator (only after M10)

Add orchestrated mode, saga state/steps, commands, compensations, recovery, dual-mode tests, and comparison ADR.

Acceptance:

- [ ] Choreography remains the default and its suite still passes.
- [ ] Orchestrated mode passes the same business invariant suite.
- [ ] Orchestrator crash/redelivery does not duplicate commands or effects.
- [ ] Saga state gives a complete operational view and uses optimistic concurrency.
- [ ] ADR fairly compares both approaches and identifies when each is preferable.

Commit: `feat(saga): add optional orchestration mode after choreography`

---

## 21. Final acceptance suite

The project is complete when the required M00–M10 evidence exists and the following succeeds from a clean checkout/reset. M11 is separately labeled optional.

1. Validate pinned tools/config and start Compose.
2. Apply every migration to empty databases.
3. Run format check and clippy with warnings denied.
4. Run all workspace unit and doc tests.
5. Run repository and messaging integration tests against real PostgreSQL/Redpanda.
6. Run the 16 deterministic invariant scenarios in section 18.
7. Run the suite again with a different recorded seed.
8. Run naive dual-write failure demo and confirm it still demonstrates both gaps.
9. Run outbox equivalent and confirm recovery/no lost logical event.
10. Run chaos smoke: broker outage/recovery, DB outage/recovery, publisher kill, consumer kill, poison message, and compensation failure.
11. Replay selected normal and DLQ events; verify no duplicate business effects.
12. Query metrics/logs/traces for the test correlation ID.
13. Restart the entire stack without wiping volumes and verify recovery.

Global pass conditions:

- All invariants I1–I15 hold.
- No stock goes negative; no duplicate authorization/refund/reservation/fulfilment occurs.
- Every accepted non-terminal order converges to `COMPLETED`, `CANCELLED`, or intentionally `MANUAL_REVIEW` within the configured bound.
- No event is acknowledged before its local transaction or DLQ handoff is durable.
- No test relies on an unexplained fixed sleep or manual database edit.
- No secrets appear in repository, logs, traces, events, or test artifacts.
- `cargo fmt`, clippy, tests, migrations, Compose health, and docs validation pass.

Store the final command transcript, versions, seeds, timestamps, and summarized results in `docs/evidence/final.md`. Do not commit huge raw logs; retain focused excerpts and reproduction commands.

---

## 22. Commit and change discipline

- Use the milestone commit subjects above; small preparatory/fix commits are acceptable but each milestone ends with one clear checkpoint.
- Do not combine future milestone behavior into an earlier commit.
- Keep migrations forward-only after a milestone is committed; do not edit applied migrations without resetting before they become shared evidence.
- Never commit `.env`, credentials, generated database volumes, target artifacts, or large logs.
- Before every milestone commit: inspect diff, format, clippy, run relevant tests, update docs/progress/evidence, and verify no unrelated changes.
- Do not rewrite user-authored changes. If the repository is already dirty, document which changes pre-existed and avoid overwriting them.

## 23. README and blog deliverables

### README must contain

- What is built and what it teaches.
- Architecture diagram and service ownership table.
- Prerequisites and five-minute quick start.
- Example create/get requests and how to follow a correlation ID.
- Choreography happy path and compensation paths.
- Naive dual-write lab and outbox recovery demo.
- Delivery-semantics statement: at-least-once transport, effectively-once effects.
- Testing and chaos commands.
- Troubleshooting, reset warning, ports, configuration, and runbook link.
- Known limitations and optional orchestrator instructions only after M11 exists.

### `docs/blog/project-2-event-driven-orders.md` must contain

- The initial mental model and why it fails.
- The two dual-write failure windows with captured evidence.
- Why the outbox deliberately permits duplicates.
- Inbox/idempotency transaction and offset timing.
- Ordering, aggregate versions, and replay trade-offs.
- Choreography and compensation complexity.
- Retry versus business failure versus poison message.
- Observability and chaos findings.
- Choreography-versus-orchestration comparison (mark planned if M11 omitted).
- Interview prompts: explain trade-offs, draw failure timelines, and propose production extensions.

## 24. Definition of done

Core Project 2 is done only when M00–M10 are complete, their acceptance evidence is truthful and reproducible, the final suite passes, documentation matches behavior, and the system demonstrates both the original inconsistency and the corrected recovery behavior. Optional M11 must never be used to conceal an incomplete choreography implementation.

When the agent believes the project is done, it must perform one last clean-state audit against every checkbox in this specification. Any unchecked or unproven required item means the project is not done; return to the earliest incomplete milestone.
