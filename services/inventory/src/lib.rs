//! The inventory service: stock and reservations (spec section 20, M04).
//!
//! Consumes `inventory.reserve_inventory` commands from
//! `inventory.commands.v1` (emitted by orders alongside `order_created` —
//! see `docs/adr/0007-orders-emits-reserve-inventory-command.md`) through
//! the idempotent-inbox protocol in [`consumer`], reserving stock
//! all-or-nothing with sorted-order row locking in [`repository`], and
//! replying with `reservation_succeeded`/`reservation_failed` via its own
//! transactional outbox (reusing `persistence::outbox`, unchanged from
//! M03). Malformed, unsupported-schema, or version-gapped commands are
//! dead-lettered rather than blocking the partition (invariant I15).

pub mod config;
pub mod consumer;
pub mod domain;
pub mod errors;
pub mod http;
pub mod repository;
