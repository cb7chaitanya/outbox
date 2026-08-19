//! The payments service: fake-provider authorization and refunds (spec
//! section 20, M05).
//!
//! Consumes `payments.authorize_payment` / `payments.refund_payment`
//! commands from `payments.commands.v1` (both share one topic; dispatch is
//! on `event_type`) through the same idempotent-inbox protocol M04
//! established for inventory, layered with a bounded, full-jitter-backoff
//! retry loop around the fake provider (spec section 15) and the
//! provider's own idempotency-key ledger ([`provider`]) so a retried call
//! never double-authorizes or double-refunds (invariant I7). Replies with
//! `payment_authorized` / `payment_failed` / `payment_refunded` via its own
//! transactional outbox.

pub mod config;
pub mod consumer;
pub mod domain;
pub mod errors;
pub mod http;
pub mod provider;
pub mod repository;
