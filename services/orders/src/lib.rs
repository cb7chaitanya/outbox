//! The orders service: order lifecycle, client idempotency, and transition
//! history (spec section 20, M01).
//!
//! No Kafka publish happens here yet — `orders` is local-consistency only
//! until M02 (naive dual write) and M03 (transactional outbox). The full
//! legal transition graph (spec section 12) is encoded in [`domain`], but
//! only `PENDING` is reachable end-to-end right now: nothing outside this
//! crate drives an order past creation until the choreographed workflow
//! (M06+) exists to react to it.

pub mod config;
pub mod domain;
pub mod errors;
pub mod http;
pub mod repository;
