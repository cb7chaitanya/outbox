//! Reusable outbox/inbox primitives shared by every service.
//!
//! Not a home for domain repositories — each service owns its own
//! aggregate persistence. The `outbox_events`, `inbox_events`, and
//! `consumer_aggregate_versions` table shapes (spec section 9) and their
//! claim/publish/replay operations land in M03 and M04.

pub const OUTBOX_TABLE: &str = "outbox_events";
pub const INBOX_TABLE: &str = "inbox_events";
