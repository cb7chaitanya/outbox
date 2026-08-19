//! Reusable outbox/inbox primitives shared by every service.
//!
//! Not a home for domain repositories — each service owns its own
//! aggregate persistence. The `outbox_events` table shape and its
//! claim/publish/backoff operations (spec section 13) land here in M03; the
//! `inbox_events`/`consumer_aggregate_versions` shapes (spec section 14)
//! land in M04.

pub mod outbox;

pub const OUTBOX_TABLE: &str = "outbox_events";
pub const INBOX_TABLE: &str = "inbox_events";
