# ADR 0001: Tech stack choices for M00 scaffolding

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M00

## Context

The spec (section 5, primary stack) mandates Rust/Tokio/Axum/SQLx/
PostgreSQL/Redpanda but leaves the Kafka client crate, config loading
crate, and a few dependency versions unspecified. M00 has to pin these at
the workspace level (section 7) before any service code compiles.

## Decision

- **Kafka/Redpanda client: `rskafka` 0.6, not `rdkafka`.** `rdkafka`
  wraps `librdkafka` in C and needs `cmake` and `pkg-config` (or a
  system-installed `librdkafka`) to build. Neither is present in this
  environment (`cmake`, `pkg-config`: not found). A scratch project
  (`rskafka` + `tokio`, `edition = "2021"`) built cleanly and quickly
  (`cargo build`, ~19s, zero errors) with no system dependencies beyond
  what rustup already provides. `rskafka` is pure Rust, async-native
  (fits Tokio directly, no blocking thread pool bridge), and speaks the
  Kafka wire protocol Redpanda implements. Tradeoff: smaller ecosystem
  and fewer production deployments than `librdkafka`-based clients, and
  some advanced consumer-group/rebalancing ergonomics that `rdkafka`
  provides may need to be hand-rolled in `messaging` later. Acceptable
  for a single-node dev broker and a learning project; revisit if a
  concrete `rskafka` limitation blocks a later milestone.
- **HTTP: `axum` 0.8** with `tower`/`tower-http` for tracing/timeout
  middleware, per spec section 5.
- **DB: `sqlx` 0.8**, `runtime-tokio-rustls` (no OpenSSL system
  dependency, consistent with avoiding native TLS build requirements),
  `postgres`, `uuid`, `chrono`, `migrate`, `macros` features.
- **Config: `figment`** with the `env` provider. Each service loads
  typed defaults then overlays `<SERVICE>_*` environment variables
  (e.g. `ORDERS_PORT`), matching `.env.example`.
- **Errors: `thiserror` 2** for typed domain/library errors, `anyhow` 1
  only at process boundaries (`main.rs`), per spec section 7.
- **IDs: `uuid` 1** with the `v7` feature — event/aggregate IDs are UUIDv7
  per the canonical envelope (spec section 8), which is time-ordered and
  avoids the index-locality problems of UUIDv4 primary keys.
- **Money: integer minor units + a 3-byte uppercase currency code** in
  `domain-common`, never floating point (spec section 9).
- **Edition: 2024**, since the pinned toolchain (rustc/cargo 1.94) fully
  supports it; `rust-toolchain.toml` pins `1.94.0` with `rustfmt` and
  `clippy` components so `cargo fmt`/`cargo clippy` are always available.

## Alternatives considered

- `rdkafka`: rejected for this environment — see above. Would be the
  default choice if `cmake`/`pkg-config`/`librdkafka` were available or
  installable; noting here so a future milestone can revisit if the
  sandbox changes or a `rskafka` gap becomes blocking.
- `kafka` (kafka-rust): rejected — effectively unmaintained, synchronous
  API that would need a blocking-thread bridge into the Tokio services.
- `config`/`envy` for configuration: `figment` chosen instead for typed
  layered sources (defaults → env) with one `extract()` call and better
  error messages; no strong reason either way, low-stakes choice.

## Consequences

- `messaging` crate wraps `rskafka` behind a `Producer`/`Consumer` port
  (see `crates/messaging/src/lib.rs`), so a future swap to another client
  only touches the adapter, not service code.
- Local dev never needs `brew install cmake` or similar; `make setup`
  only requires `cargo` and `docker`.
- If a later milestone (consumer groups, precise partition assignment
  for M04's idempotent inbox, or M08's replay CLI) hits a real `rskafka`
  ergonomics gap, re-open this ADR rather than silently reintroducing
  `rdkafka` — document the concrete gap first.
