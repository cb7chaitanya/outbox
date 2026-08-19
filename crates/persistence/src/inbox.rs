//! Idempotent-inbox primitives shared by every consuming service (spec
//! section 14): the `(consumer_name, event_id)` uniqueness check backing
//! invariant I11, and per-aggregate version bookkeeping for the
//! stale/gap/apply ordering policy (invariants I12/I13).
//!
//! This project's Kafka client (`rskafka`) has no broker-managed consumer
//! group / offset-commit protocol (see `docs/adr/0001-tech-stack-choices.md`).
//! [`fetch_offset`]/[`commit_offset`] are this project's local stand-in for
//! that: a durable ledger row advanced only after the handler's business
//! transaction commits, giving the same "never ack before local commit"
//! guarantee spec section 14 asks for — see
//! `docs/adr/0006-inbox-consumer-offset-ledger.md`.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct InboxRow {
    pub consumer_name: String,
    pub event_id: Uuid,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub aggregate_id: Uuid,
    pub aggregate_version: i64,
    pub received_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub payload_hash: String,
}

#[derive(Debug, Clone)]
pub struct NewInboxEntry {
    pub consumer_name: String,
    pub event_id: Uuid,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub aggregate_id: Uuid,
    pub aggregate_version: i64,
    pub payload_hash: String,
}

/// Attempts to claim this event id for this consumer (spec section 14
/// point 2). `Ok(true)` means this call is the first to see it and
/// business work should proceed; `Ok(false)` means a row already existed
/// — the caller must then [`fetch`] it and compare `payload_hash` to
/// decide duplicate-ack (match) vs poison/DLQ (mismatch, point 3).
pub async fn try_claim(
    conn: &mut PgConnection,
    now: DateTime<Utc>,
    entry: &NewInboxEntry,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "insert into inbox_events \
         (consumer_name, event_id, source_topic, source_partition, source_offset, \
          aggregate_id, aggregate_version, received_at, payload_hash) \
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         on conflict (consumer_name, event_id) do nothing",
    )
    .bind(&entry.consumer_name)
    .bind(entry.event_id)
    .bind(&entry.source_topic)
    .bind(entry.source_partition)
    .bind(entry.source_offset)
    .bind(entry.aggregate_id)
    .bind(entry.aggregate_version)
    .bind(now)
    .bind(&entry.payload_hash)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn fetch(
    pool: &PgPool,
    consumer_name: &str,
    event_id: Uuid,
) -> Result<Option<InboxRow>, sqlx::Error> {
    sqlx::query_as::<_, InboxRow>(
        "select consumer_name, event_id, source_topic, source_partition, source_offset, \
         aggregate_id, aggregate_version, received_at, processed_at, payload_hash \
         from inbox_events where consumer_name = $1 and event_id = $2",
    )
    .bind(consumer_name)
    .bind(event_id)
    .fetch_optional(pool)
    .await
}

pub async fn mark_processed(
    conn: &mut PgConnection,
    consumer_name: &str,
    event_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update inbox_events set processed_at = $1 where consumer_name = $2 and event_id = $3",
    )
    .bind(now)
    .bind(consumer_name)
    .bind(event_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// The ordering/replay policy from spec section 14: an aggregate with no
/// recorded `last_version` is treated as version 0, so the first event
/// (version 1) always applies cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionDecision {
    Apply,
    Stale,
    Gap,
}

pub async fn version_decision(
    conn: &mut PgConnection,
    consumer_name: &str,
    aggregate_id: Uuid,
    incoming_version: i64,
) -> Result<VersionDecision, sqlx::Error> {
    let last: Option<i64> = sqlx::query_scalar(
        "select last_version from consumer_aggregate_versions \
         where consumer_name = $1 and aggregate_id = $2",
    )
    .bind(consumer_name)
    .bind(aggregate_id)
    .fetch_optional(&mut *conn)
    .await?;
    let last = last.unwrap_or(0);
    Ok(if incoming_version == last + 1 {
        VersionDecision::Apply
    } else if incoming_version <= last {
        VersionDecision::Stale
    } else {
        VersionDecision::Gap
    })
}

pub async fn advance_version(
    conn: &mut PgConnection,
    consumer_name: &str,
    aggregate_id: Uuid,
    new_version: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into consumer_aggregate_versions (consumer_name, aggregate_id, last_version) \
         values ($1, $2, $3) \
         on conflict (consumer_name, aggregate_id) do update set last_version = excluded.last_version",
    )
    .bind(consumer_name)
    .bind(aggregate_id)
    .bind(new_version)
    .execute(conn)
    .await?;
    Ok(())
}

/// Local offset ledger (see module docs). `fetch_offset` returns `0`
/// (start of topic) for a consumer/topic/partition never seen before.
pub async fn fetch_offset(
    pool: &PgPool,
    consumer_name: &str,
    topic: &str,
    partition: i32,
) -> Result<i64, sqlx::Error> {
    let offset: Option<i64> = sqlx::query_scalar(
        "select next_offset from consumer_offsets \
         where consumer_name = $1 and topic = $2 and partition = $3",
    )
    .bind(consumer_name)
    .bind(topic)
    .bind(partition)
    .fetch_optional(pool)
    .await?;
    Ok(offset.unwrap_or(0))
}

/// Advances the ledger to `next_offset` (the offset to resume from on the
/// next fetch). Callers must only invoke this after the corresponding
/// handler's business transaction has committed (or, for a poison message,
/// after the DLQ publish has been acknowledged) — never before.
pub async fn commit_offset(
    pool: &PgPool,
    consumer_name: &str,
    topic: &str,
    partition: i32,
    next_offset: i64,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into consumer_offsets (consumer_name, topic, partition, next_offset, updated_at) \
         values ($1, $2, $3, $4, $5) \
         on conflict (consumer_name, topic, partition) \
         do update set next_offset = excluded.next_offset, updated_at = excluded.updated_at",
    )
    .bind(consumer_name)
    .bind(topic)
    .bind(partition)
    .bind(next_offset)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::VersionDecision;

    // version_decision's branch logic (apply/stale/gap) is exercised
    // end-to-end against a real database in inventory's integration tests,
    // since it reads through a live connection; this module has no pure
    // logic worth unit-testing in isolation beyond the type itself.
    #[test]
    fn version_decision_variants_are_distinct() {
        assert_ne!(VersionDecision::Apply, VersionDecision::Stale);
        assert_ne!(VersionDecision::Stale, VersionDecision::Gap);
    }
}
