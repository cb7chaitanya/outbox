# Scripts

- `postgres-init/` — Compose-time database bootstrap (runs automatically
  via `docker-entrypoint-initdb.d`), creates one database per service.
- `demo-dual-write-failure.sh` — lands with M02, runs the two naive
  dual-write fault demonstrations and prints invariant violations.
- Additional chaos/demo scripts land with M09.
