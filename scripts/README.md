# Scripts

- `postgres-init/` — Compose-time database bootstrap (runs automatically
  via `docker-entrypoint-initdb.d`), creates one database per service.
- `demo-dual-write-failure.sh` — runs the two naive
  dual-write fault demonstrations and prints invariant violations.
- `chaos-smoke.sh` — runs bounded broker/DB outage, worker restart,
  poison-isolation, correlation, and metrics checks.
