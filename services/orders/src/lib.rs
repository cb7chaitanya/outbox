//! The orders service: order lifecycle, client idempotency, transition
//! history (spec section 20, M01); the naive dual-write publish path with
//! deterministic fault injection (M02); and the transactional outbox that
//! replaces it as the default (M03).
//!
//! `DELIVERY_MODE=outbox` (the current default) inserts the business
//! mutation and its outbox event in one database transaction
//! (`repository::create_order`'s `build_outbox_event` closure), and a
//! background worker (`persistence::outbox::spawn_publisher_loop`,
//! started from `main`) claims and publishes rows independently of the
//! request path — see `docs/adr/0005-outbox-claim-lease-and-backoff-design.md`.
//! `DELIVERY_MODE=naive` remains fully runnable: it publishes
//! `orders.order_created` directly to Kafka right after the DB commit,
//! with no shared transaction — see `http::publish_naive` and
//! `docs/failure-lab.md` for the two atomicity gaps this deliberately
//! reproduces, and `outbox_tests.rs` for how the outbox mode closes them.
//! The full legal transition graph (spec section 12) is encoded in
//! [`domain`], but only `PENDING` is reachable end-to-end right now:
//! nothing outside this crate drives an order past creation until the
//! choreographed workflow (M06+) exists to react to it.

pub mod config;
pub mod domain;
pub mod errors;
pub mod http;
pub mod repository;
