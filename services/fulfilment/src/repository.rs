//! Postgres-backed persistence for the fulfilment aggregate.
//!
//! `record_outcome` is idempotent the same way `payments::repository::
//! find_by_order` and `inventory::repository::reserve` are: `order_id` is
//! unique on `fulfilments`, so a redelivered `create_fulfilment` for an
//! already-decided order replays the original outcome instead of creating
//! (or fault-failing) a second fulfilment (invariant I8's "at most once"
//! shape, applied to fulfilment).

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::FulfilmentStatus;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FulfilmentRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub reservation_id: Uuid,
    pub payment_id: Uuid,
    pub status: FulfilmentStatus,
    pub failure_code: Option<String>,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RecordOutcome {
    pub fulfilment_id: Uuid,
    pub status: FulfilmentStatus,
    pub failure_code: Option<String>,
    /// `true` if this call decided the outcome; `false` if it replayed an
    /// already-decided fulfilment for this `order_id`.
    pub created: bool,
}

/// Takes an already-open transaction connection rather than a pool, for
/// the same reason `inventory::repository::reserve` does: the caller (the
/// fulfilment consumer) must apply this mutation and its resulting outbox
/// event, inbox mark, and consumer-version advance in one transaction
/// (spec section 14 steps 4-6).
pub async fn record_outcome(
    conn: &mut PgConnection,
    order_id: Uuid,
    reservation_id: Uuid,
    payment_id: Uuid,
    status: FulfilmentStatus,
    failure_code: Option<&str>,
    now: DateTime<Utc>,
) -> Result<RecordOutcome, sqlx::Error> {
    if let Some(existing) = sqlx::query_as::<_, FulfilmentRow>(
        "select id, order_id, reservation_id, payment_id, status, failure_code, version, \
         created_at, updated_at from fulfilments where order_id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(RecordOutcome {
            fulfilment_id: existing.id,
            status: existing.status,
            failure_code: existing.failure_code,
            created: false,
        });
    }

    let fulfilment_id = Uuid::now_v7();
    sqlx::query(
        "insert into fulfilments \
         (id, order_id, reservation_id, payment_id, status, failure_code, version, created_at, updated_at) \
         values ($1, $2, $3, $4, $5, $6, 1, $7, $7)",
    )
    .bind(fulfilment_id)
    .bind(order_id)
    .bind(reservation_id)
    .bind(payment_id)
    .bind(status)
    .bind(failure_code)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(RecordOutcome {
        fulfilment_id,
        status,
        failure_code: failure_code.map(str::to_string),
        created: true,
    })
}
