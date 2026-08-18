//! Postgres-backed persistence for the order aggregate.
//!
//! `create_order` implements the race-safe idempotency-key insert pattern
//! (spec section 10): `INSERT ... ON CONFLICT (idempotency_key) DO
//! NOTHING` blocks concurrent duplicate-key inserts on the unique index
//! until the winner commits or aborts, so a losing caller can safely read
//! back an already-committed row afterward — no check-then-insert race.
//!
//! `transition_order` implements the optimistic-concurrency pattern from
//! section 9: an `UPDATE ... WHERE id = $1 AND version = $expected` that
//! affects zero rows is a typed `VersionConflict`, never an implicit
//! success, and an out-of-graph transition is rejected before any write
//! happens at all.

use chrono::{DateTime, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::{NormalizedOrder, OrderStatus, is_legal_transition};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    pub idempotency_key: String,
    pub idempotency_request_hash: String,
    pub status: OrderStatus,
    pub currency: String,
    pub amount_minor: i64,
    pub version: i64,
    pub cancellation_reason: Option<String>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderItemRow {
    pub sku: String,
    pub quantity: i64,
    pub unit_price_minor: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TransitionRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub from_status: Option<OrderStatus>,
    pub to_status: OrderStatus,
    pub reason: Option<String>,
    pub triggering_event_id: Option<Uuid>,
    pub order_version: i64,
    pub created_at: DateTime<Utc>,
}

const ORDER_COLUMNS: &str = "id, idempotency_key, idempotency_request_hash, status, \
     currency, amount_minor, version, cancellation_reason, correlation_id, created_at, updated_at";

#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("idempotency key reused with a different request body")]
    IdempotencyKeyReused,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum TransitionError {
    #[error("order not found")]
    NotFound,
    #[error("illegal transition from {from} to {to}")]
    IllegalTransition { from: OrderStatus, to: OrderStatus },
    #[error("version conflict: expected version {expected}")]
    VersionConflict { expected: i64 },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug)]
pub struct CreateOutcome {
    pub order: OrderRow,
    pub items: Vec<OrderItemRow>,
    /// `true` if this call created the order; `false` if it replayed an
    /// already-committed idempotent request.
    pub created: bool,
}

/// Canonical fingerprint of a normalized request: sorted items plus
/// currency, hashed so idempotency-key replay can detect a byte-different
/// request body reusing the same key (spec section 10).
fn fingerprint(normalized: &NormalizedOrder) -> String {
    let canonical = json!({
        "currency": normalized.currency,
        "items": normalized.items.iter().map(|item| json!({
            "sku": item.sku,
            "quantity": item.quantity,
            "unit_price_minor": item.unit_price_minor,
        })).collect::<Vec<_>>(),
    });
    let bytes = serde_json::to_vec(&canonical).expect("canonical json serialization cannot fail");
    hex::encode(Sha256::digest(bytes))
}

pub async fn create_order(
    pool: &PgPool,
    idempotency_key: &str,
    normalized: &NormalizedOrder,
    correlation_id: Uuid,
    now: DateTime<Utc>,
) -> Result<CreateOutcome, RepoError> {
    let request_hash = fingerprint(normalized);
    let order_id = Uuid::now_v7();

    let mut tx = pool.begin().await?;

    let insert_sql = format!(
        "insert into orders (id, idempotency_key, idempotency_request_hash, status, \
         currency, amount_minor, version, correlation_id, created_at, updated_at) \
         values ($1, $2, $3, 'PENDING', $4, $5, 1, $6, $7, $7) \
         on conflict (idempotency_key) do nothing \
         returning {ORDER_COLUMNS}"
    );
    let inserted = sqlx::query_as::<_, OrderRow>(&insert_sql)
        .bind(order_id)
        .bind(idempotency_key)
        .bind(&request_hash)
        .bind(&normalized.currency)
        .bind(normalized.amount_minor)
        .bind(correlation_id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;

    let Some(order) = inserted else {
        // Lost the race (or this is a genuine replay): the ON CONFLICT DO
        // NOTHING above blocked on the unique index until the winner
        // committed, so the row below is guaranteed to be visible now.
        drop(tx);
        let existing = sqlx::query_as::<_, OrderRow>(&format!(
            "select {ORDER_COLUMNS} from orders where idempotency_key = $1"
        ))
        .bind(idempotency_key)
        .fetch_one(pool)
        .await?;
        if existing.idempotency_request_hash != request_hash {
            return Err(RepoError::IdempotencyKeyReused);
        }
        let items = fetch_items(pool, existing.id).await?;
        return Ok(CreateOutcome {
            order: existing,
            items,
            created: false,
        });
    };

    for item in &normalized.items {
        sqlx::query(
            "insert into order_items (order_id, sku, quantity, unit_price_minor) \
             values ($1, $2, $3, $4)",
        )
        .bind(order.id)
        .bind(&item.sku)
        .bind(item.quantity)
        .bind(item.unit_price_minor)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        "insert into order_transitions \
         (id, order_id, from_status, to_status, reason, triggering_event_id, order_version, created_at) \
         values ($1, $2, NULL, 'PENDING', $3, NULL, 1, $4)",
    )
    .bind(Uuid::now_v7())
    .bind(order.id)
    .bind("order created")
    .bind(now)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    let items = normalized
        .items
        .iter()
        .map(|item| OrderItemRow {
            sku: item.sku.clone(),
            quantity: item.quantity,
            unit_price_minor: item.unit_price_minor,
        })
        .collect();

    Ok(CreateOutcome {
        order,
        items,
        created: true,
    })
}

async fn fetch_items(pool: &PgPool, order_id: Uuid) -> Result<Vec<OrderItemRow>, sqlx::Error> {
    sqlx::query_as::<_, OrderItemRow>(
        "select sku, quantity, unit_price_minor from order_items where order_id = $1 order by sku",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
}

pub async fn get_order(
    pool: &PgPool,
    order_id: Uuid,
) -> Result<Option<(OrderRow, Vec<OrderItemRow>)>, sqlx::Error> {
    let order =
        sqlx::query_as::<_, OrderRow>(&format!("select {ORDER_COLUMNS} from orders where id = $1"))
            .bind(order_id)
            .fetch_optional(pool)
            .await?;
    match order {
        Some(order) => {
            let items = fetch_items(pool, order_id).await?;
            Ok(Some((order, items)))
        }
        None => Ok(None),
    }
}

/// `None` if the order does not exist; `Some(vec![])` is impossible in
/// practice since every order gets a creation transition, but the type
/// still distinguishes "no such order" from "no transitions yet".
pub async fn get_transitions(
    pool: &PgPool,
    order_id: Uuid,
) -> Result<Option<Vec<TransitionRow>>, sqlx::Error> {
    let exists: bool = sqlx::query_scalar("select exists(select 1 from orders where id = $1)")
        .bind(order_id)
        .fetch_one(pool)
        .await?;
    if !exists {
        return Ok(None);
    }
    let rows = sqlx::query_as::<_, TransitionRow>(
        "select id, order_id, from_status, to_status, reason, triggering_event_id, order_version, created_at \
         from order_transitions where order_id = $1 order by order_version asc",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await?;
    Ok(Some(rows))
}

/// Applies a single legal, version-guarded transition. Used directly by
/// tests to demonstrate illegal and stale transitions are rejected with no
/// partial writes (spec M01 acceptance); no M01 HTTP endpoint exposes
/// arbitrary transitions yet (`POST /cancel` is optional and deferred).
pub async fn transition_order(
    pool: &PgPool,
    order_id: Uuid,
    expected_version: i64,
    to: OrderStatus,
    reason: Option<&str>,
    now: DateTime<Utc>,
) -> Result<OrderRow, TransitionError> {
    let mut tx = pool.begin().await?;

    let current =
        sqlx::query_as::<_, OrderRow>(&format!("select {ORDER_COLUMNS} from orders where id = $1"))
            .bind(order_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(current) = current else {
        return Err(TransitionError::NotFound);
    };

    if !is_legal_transition(current.status, to) {
        return Err(TransitionError::IllegalTransition {
            from: current.status,
            to,
        });
    }

    let update_sql = format!(
        "update orders set status = $1, version = version + 1, updated_at = $2 \
         where id = $3 and version = $4 \
         returning {ORDER_COLUMNS}"
    );
    let updated = sqlx::query_as::<_, OrderRow>(&update_sql)
        .bind(to)
        .bind(now)
        .bind(order_id)
        .bind(expected_version)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(updated) = updated else {
        return Err(TransitionError::VersionConflict {
            expected: expected_version,
        });
    };

    sqlx::query(
        "insert into order_transitions \
         (id, order_id, from_status, to_status, reason, triggering_event_id, order_version, created_at) \
         values ($1, $2, $3, $4, $5, NULL, $6, $7)",
    )
    .bind(Uuid::now_v7())
    .bind(order_id)
    .bind(current.status)
    .bind(to)
    .bind(reason)
    .bind(updated.version)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(updated)
}
