//! The orders service: order lifecycle, client idempotency, transition
//! history (spec section 20, M01), and the naive dual-write publish path
//! with deterministic fault injection (M02).
//!
//! `DELIVERY_MODE=naive` (the current default) publishes
//! `orders.order_created` directly to Kafka right after the DB commit,
//! with no shared transaction and no outbox — see `http::publish_naive`
//! and `docs/failure-lab.md` for the two atomicity gaps this deliberately
//! reproduces. `DELIVERY_MODE=outbox` parses but has no implementation
//! until M03. The full legal transition graph (spec section 12) is
//! encoded in [`domain`], but only `PENDING` is reachable end-to-end right
//! now: nothing outside this crate drives an order past creation until the
//! choreographed workflow (M06+) exists to react to it.

pub mod config;
pub mod domain;
pub mod errors;
pub mod http;
pub mod repository;
