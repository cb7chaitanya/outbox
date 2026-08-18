# Migrations

Each service owns its schema exclusively (spec section 6: "No service reads
or writes another service's tables. Cross-service joins are forbidden.").
There is no shared migration directory at the repository root — this file
only documents the convention; actual SQL lives per service:

```text
services/orders/migrations/
services/inventory/migrations/
services/payments/migrations/
services/fulfilment/migrations/
services/saga-orchestrator/migrations/   # absent until M11
```

Rules:

- Each service connects with its own SQLx `PgPool` to its own database
  (see `.env.example` for the four local database names) and runs its own
  `sqlx migrate run` against only that database.
- Migrations are forward-only once a milestone that depends on them is
  committed (spec section 22). Do not edit an applied migration; add a new
  one.
- Shared table *shapes* (`outbox_events`, `inbox_events`,
  `consumer_aggregate_versions` — spec section 9) are duplicated per
  service migration directory rather than referenced from a shared schema,
  because each service's copy lives in its own database.

No service migrations exist yet as of M00 — they are added starting M01
(orders) and M04 (inventory).
