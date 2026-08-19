//! Transactional outbox primitives shared by every producing service (spec
//! section 13, table shape in section 9). A service inserts its own
//! outbox row inside the same SQLx transaction as the business mutation
//! that requires it (invariant I3) using [`insert`]; a background worker
//! built from [`claim_batch`], [`mark_published`], and [`mark_failed`]
//! (or the ready-made [`spawn_publisher_loop`]) publishes independently of
//! the request path.
//!
//! This module only knows the generic outbox row shape — `envelope` is an
//! opaque `serde_json::Value`. Concrete event types live in `contracts`
//! and are the producing service's concern, not this crate's.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use messaging::Producer;
use rand::Rng;
use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use test_support::FaultInjector;
use uuid::Uuid;

/// A new outbox row to insert alongside a business mutation, in the same
/// transaction. `id` is generated once by the caller before the
/// transaction and stays stable across any caller-level retry (spec
/// section 13).
#[derive(Debug, Clone)]
pub struct NewOutboxEvent {
    pub id: Uuid,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub aggregate_version: i64,
    pub topic: String,
    pub message_key: String,
    pub envelope: Value,
}

/// Inserts one outbox row using the caller's open transaction connection.
/// Never call this outside a transaction that also writes the business
/// mutation — that pairing is the entire point of the outbox pattern.
pub async fn insert(
    conn: &mut PgConnection,
    now: DateTime<Utc>,
    event: &NewOutboxEvent,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into outbox_events \
         (id, aggregate_type, aggregate_id, aggregate_version, topic, message_key, envelope, \
          created_at, next_attempt_at, attempts) \
         values ($1, $2, $3, $4, $5, $6, $7, $8, $8, 0)",
    )
    .bind(event.id)
    .bind(&event.aggregate_type)
    .bind(event.aggregate_id)
    .bind(event.aggregate_version)
    .bind(&event.topic)
    .bind(&event.message_key)
    .bind(&event.envelope)
    .bind(now)
    .execute(conn)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClaimedOutboxEvent {
    pub id: Uuid,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub aggregate_version: i64,
    pub topic: String,
    pub message_key: String,
    pub envelope: Value,
    pub created_at: DateTime<Utc>,
    pub attempts: i32,
    /// `true` when this row already had a (now-expired) claim from a
    /// previous worker — i.e. this claim is a lease recovery, not a first
    /// claim (spec section 13 point 6, spec section 16's "lease recoveries"
    /// metric).
    pub was_previously_claimed: bool,
}

/// Atomically claims up to `batch_size` eligible unpublished rows using
/// `FOR UPDATE SKIP LOCKED` (spec section 13 point 1-2): concurrent callers
/// against the same table never claim the same row, and a row already
/// claimed by another live worker (an unexpired lease) is skipped rather
/// than blocked on.
pub async fn claim_batch(
    pool: &PgPool,
    claimed_by: &str,
    lease: ChronoDuration,
    batch_size: i64,
    now: DateTime<Utc>,
) -> Result<Vec<ClaimedOutboxEvent>, sqlx::Error> {
    let claimed_until = now + lease;
    sqlx::query_as::<_, ClaimedOutboxEvent>(
        "with claimable as ( \
           select id, claimed_by as previous_claimed_by \
           from outbox_events \
           where published_at is null \
             and next_attempt_at <= $1 \
             and (claimed_until is null or claimed_until < $1) \
           order by created_at, id \
           for update skip locked \
           limit $2 \
         ) \
         update outbox_events o \
         set claimed_by = $3, claimed_until = $4 \
         from claimable c \
         where o.id = c.id \
         returning o.id, o.aggregate_type, o.aggregate_id, o.aggregate_version, o.topic, \
           o.message_key, o.envelope, o.created_at, o.attempts, \
           (c.previous_claimed_by is not null) as was_previously_claimed",
    )
    .bind(now)
    .bind(batch_size)
    .bind(claimed_by)
    .bind(claimed_until)
    .fetch_all(pool)
    .await
}

pub async fn mark_published(pool: &PgPool, id: Uuid, now: DateTime<Utc>) -> Result<(), sqlx::Error> {
    sqlx::query("update outbox_events set published_at = $1 where id = $2")
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Records a publish failure, releases the claim (so `next_attempt_at`
/// alone governs the next eligibility check rather than waiting out the
/// full lease), and schedules a full-jitter backoff retry.
pub async fn mark_failed(
    pool: &PgPool,
    id: Uuid,
    next_attempt_at: DateTime<Utc>,
    error: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update outbox_events \
         set attempts = attempts + 1, last_error = $1, next_attempt_at = $2, \
             claimed_by = null, claimed_until = null \
         where id = $3",
    )
    .bind(error)
    .bind(next_attempt_at)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BacklogMetrics {
    pub unpublished_count: i64,
    pub oldest_unpublished_age_seconds: Option<f64>,
}

/// Backs the `/metrics` backlog-count and oldest-unpublished-age gauges
/// (spec section 16).
pub async fn backlog_metrics(pool: &PgPool, now: DateTime<Utc>) -> Result<BacklogMetrics, sqlx::Error> {
    let row: (i64, Option<DateTime<Utc>>) = sqlx::query_as(
        "select count(*), min(created_at) from outbox_events where published_at is null",
    )
    .fetch_one(pool)
    .await?;
    let oldest_unpublished_age_seconds = row
        .1
        .map(|oldest| (now - oldest).num_milliseconds().max(0) as f64 / 1000.0);
    Ok(BacklogMetrics {
        unpublished_count: row.0,
        oldest_unpublished_age_seconds,
    })
}

/// Full-jitter exponential backoff (spec section 15): a random delay in
/// `[0, min(cap, base * 2^attempts)]`. `attempts` is the failure count
/// *before* this one (so the first failure uses `attempts == 0`).
pub fn full_jitter_backoff(attempts: u32, base: StdDuration, cap: StdDuration) -> StdDuration {
    let exp = base.saturating_mul(1u32.checked_shl(attempts).unwrap_or(u32::MAX));
    let upper = exp.min(cap);
    if upper.is_zero() {
        return StdDuration::ZERO;
    }
    rand::thread_rng().gen_range(StdDuration::ZERO..=upper)
}

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    pub claimed_by: String,
    pub batch_size: i64,
    pub lease: ChronoDuration,
    pub poll_interval: StdDuration,
    pub backoff_base: StdDuration,
    pub backoff_cap: StdDuration,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            claimed_by: format!("publisher-{}", Uuid::now_v7()),
            batch_size: 20,
            lease: ChronoDuration::seconds(30),
            poll_interval: StdDuration::from_millis(200),
            backoff_base: StdDuration::from_millis(100),
            backoff_cap: StdDuration::from_secs(30),
        }
    }
}

/// Counters backing the publisher's `/metrics` output (spec section 16):
/// publish attempts, failures, and lease recoveries. Backlog count and
/// oldest-unpublished age are queried live from the database instead
/// (see [`backlog_metrics`]) since they're a property of the table, not
/// the worker process.
#[derive(Debug, Default)]
pub struct PublishMetrics {
    pub attempts_total: AtomicU64,
    pub failures_total: AtomicU64,
    pub lease_recoveries_total: AtomicU64,
    pub published_total: AtomicU64,
}

impl PublishMetrics {
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.attempts_total.load(Ordering::Relaxed),
            self.failures_total.load(Ordering::Relaxed),
            self.lease_recoveries_total.load(Ordering::Relaxed),
            self.published_total.load(Ordering::Relaxed),
        )
    }
}

/// Fault point (spec section 17: "after broker publish/before outbox
/// mark-published"): fires after a successful `producer.publish` but
/// before the row is marked published, simulating a worker crash in that
/// window. The row is left claimed and unpublished; it is only
/// republished once the lease expires and another `claim_batch` call
/// picks it back up (spec section 13 point 6) — that redelivery is the
/// documented, expected behavior, not a bug.
pub const FAULT_AFTER_BROKER_PUBLISH_BEFORE_MARK_PUBLISHED: &str =
    "orders.outbox.after_broker_publish_before_mark_published";

fn headers_from_envelope(event_id: Uuid, envelope: &Value) -> Vec<(String, Vec<u8>)> {
    let mut headers = vec![("event_id".to_string(), event_id.to_string().into_bytes())];
    for field in ["correlation_id", "causation_id", "producer"] {
        if let Some(v) = envelope.get(field).and_then(Value::as_str) {
            headers.push((field.to_string(), v.as_bytes().to_vec()));
        }
    }
    headers
}

/// Claims and attempts to publish one batch. Returns the number of rows
/// claimed (0 means the backlog was empty *or* every eligible row is
/// already claimed by a live lease). A row that publishes successfully is
/// marked published; a row whose publish fails has its failure recorded
/// and a backoff retry scheduled. The fault injector is checked once per
/// row, after a successful publish and before the mark-published write —
/// see [`FAULT_AFTER_BROKER_PUBLISH_BEFORE_MARK_PUBLISHED`].
pub async fn run_publisher_once(
    pool: &PgPool,
    producer: &dyn Producer,
    fault_injector: &FaultInjector,
    config: &PublisherConfig,
    metrics: &PublishMetrics,
    now: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let claimed = claim_batch(pool, &config.claimed_by, config.lease, config.batch_size, now).await?;
    let count = claimed.len();

    for row in claimed {
        if row.was_previously_claimed {
            metrics.lease_recoveries_total.fetch_add(1, Ordering::Relaxed);
        }
        metrics.attempts_total.fetch_add(1, Ordering::Relaxed);

        let headers = headers_from_envelope(row.id, &row.envelope);
        let payload = serde_json::to_vec(&row.envelope).expect("stored envelope is valid json");

        match producer
            .publish(&row.topic, &row.message_key, payload, headers)
            .await
        {
            Ok(()) => {
                if fault_injector
                    .maybe_fail(
                        FAULT_AFTER_BROKER_PUBLISH_BEFORE_MARK_PUBLISHED,
                        Some(&row.id.to_string()),
                    )
                    .await
                    .is_err()
                {
                    // Simulated crash: leave the row claimed and
                    // unpublished. It becomes eligible again once its
                    // lease expires.
                    continue;
                }
                mark_published(pool, row.id, Utc::now()).await?;
                metrics.published_total.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                metrics.failures_total.fetch_add(1, Ordering::Relaxed);
                let backoff = full_jitter_backoff(row.attempts as u32, config.backoff_base, config.backoff_cap);
                let next_attempt_at = Utc::now()
                    + ChronoDuration::from_std(backoff).unwrap_or(ChronoDuration::zero());
                mark_failed(pool, row.id, next_attempt_at, &err.to_string()).await?;
            }
        }
    }

    Ok(count)
}

/// Runs [`run_publisher_once`] forever on a background task, polling at
/// `config.poll_interval` when a batch comes back empty and immediately
/// retrying when it doesn't (to drain a backlog quickly). Errors from the
/// claim query itself (e.g. a lost DB connection) are logged and treated
/// like an empty batch — the next tick tries again rather than exiting the
/// loop.
pub fn spawn_publisher_loop(
    pool: PgPool,
    producer: Arc<dyn Producer>,
    fault_injector: Arc<FaultInjector>,
    config: PublisherConfig,
    metrics: Arc<PublishMetrics>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let outcome =
                run_publisher_once(&pool, producer.as_ref(), &fault_injector, &config, &metrics, Utc::now())
                    .await;
            match outcome {
                Ok(n) if n > 0 => continue,
                Ok(_) => {}
                Err(err) => tracing::error!(error = %err, "outbox publisher batch failed"),
            }
            tokio::time::sleep(config.poll_interval).await;
        }
    })
}
