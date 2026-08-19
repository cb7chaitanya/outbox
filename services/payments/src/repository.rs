//! Postgres-backed persistence for the payment aggregate and this
//! service's own record of what it asked the provider to do
//! (`payment_operations` — distinct from the provider's internal ledger in
//! [`crate::provider`]).
//!
//! Every function here takes an already-open transaction connection: the
//! consumer handler owns the transaction boundary so the payment row, the
//! operation bookkeeping row, the resulting outbox event, the inbox mark,
//! and the consumer-version advance all commit or roll back together
//! (spec section 14 steps 4-6).

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::domain::{OperationStatus, OperationType, PaymentStatus};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PaymentRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub currency: String,
    pub amount_minor: i64,
    pub status: PaymentStatus,
    pub provider_reference: Option<String>,
    pub version: i64,
    pub failure_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const PAYMENT_COLUMNS: &str = "id, order_id, currency, amount_minor, status, \
     provider_reference, version, failure_code, created_at, updated_at";

/// `order_id` is unique, so a redelivered `authorize_payment`/
/// `refund_payment` command for an already-decided order finds its
/// existing row here instead of re-invoking the provider — the outer
/// idempotency layer, complementing the inbox's event-id dedup and the
/// provider's own idempotency-key ledger.
pub async fn find_by_order(
    conn: &mut PgConnection,
    order_id: Uuid,
) -> Result<Option<PaymentRow>, sqlx::Error> {
    sqlx::query_as::<_, PaymentRow>(&format!(
        "select {PAYMENT_COLUMNS} from payments where order_id = $1"
    ))
    .bind(order_id)
    .fetch_optional(conn)
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn record_authorized(
    conn: &mut PgConnection,
    payment_id: Uuid,
    order_id: Uuid,
    currency: &str,
    amount_minor: i64,
    provider_reference: &str,
    now: DateTime<Utc>,
) -> Result<PaymentRow, sqlx::Error> {
    sqlx::query_as::<_, PaymentRow>(&format!(
        "insert into payments (id, order_id, currency, amount_minor, status, \
         provider_reference, version, created_at, updated_at) \
         values ($1, $2, $3, $4, 'AUTHORIZED', $5, 1, $6, $6) \
         returning {PAYMENT_COLUMNS}"
    ))
    .bind(payment_id)
    .bind(order_id)
    .bind(currency)
    .bind(amount_minor)
    .bind(provider_reference)
    .bind(now)
    .fetch_one(conn)
    .await
}

pub async fn record_declined(
    conn: &mut PgConnection,
    payment_id: Uuid,
    order_id: Uuid,
    currency: &str,
    amount_minor: i64,
    failure_code: &str,
    now: DateTime<Utc>,
) -> Result<PaymentRow, sqlx::Error> {
    sqlx::query_as::<_, PaymentRow>(&format!(
        "insert into payments (id, order_id, currency, amount_minor, status, \
         failure_code, version, created_at, updated_at) \
         values ($1, $2, $3, $4, 'FAILED', $5, 1, $6, $6) \
         returning {PAYMENT_COLUMNS}"
    ))
    .bind(payment_id)
    .bind(order_id)
    .bind(currency)
    .bind(amount_minor)
    .bind(failure_code)
    .bind(now)
    .fetch_one(conn)
    .await
}

/// Idempotent: if `payment` is already `REFUNDED`, returns it unchanged
/// rather than re-applying (spec section 12's compensation matrix:
/// "refunding an already-refunded payment return logical success without
/// repeating the external effect").
pub async fn record_refunded(
    conn: &mut PgConnection,
    payment: &PaymentRow,
    now: DateTime<Utc>,
) -> Result<PaymentRow, sqlx::Error> {
    if payment.status == PaymentStatus::Refunded {
        return Ok(payment.clone());
    }
    sqlx::query_as::<_, PaymentRow>(&format!(
        "update payments set status = 'REFUNDED', version = version + 1, updated_at = $2 \
         where id = $1 \
         returning {PAYMENT_COLUMNS}"
    ))
    .bind(payment.id)
    .bind(now)
    .fetch_one(conn)
    .await
}

/// Records this service's own attempt bookkeeping for one provider
/// operation (spec section 9's `payment_operations` table). `idempotency_key`
/// is unique, so calling this twice for the same key updates `attempts`
/// and `status` in place rather than inserting a second row — a message
/// that DLQ's-and-is-later-replayed after the row already exists still
/// records cleanly.
pub async fn record_operation(
    conn: &mut PgConnection,
    payment_id: Uuid,
    operation_type: OperationType,
    idempotency_key: &str,
    status: OperationStatus,
    attempts: i32,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into payment_operations \
         (id, payment_id, operation_type, idempotency_key, status, attempts, created_at, updated_at) \
         values ($1, $2, $3, $4, $5, $6, $7, $7) \
         on conflict (idempotency_key) do update set \
         status = excluded.status, attempts = payment_operations.attempts + excluded.attempts, \
         updated_at = excluded.updated_at",
    )
    .bind(Uuid::now_v7())
    .bind(payment_id)
    .bind(operation_type)
    .bind(idempotency_key)
    .bind(status)
    .bind(attempts)
    .bind(now)
    .execute(conn)
    .await?;
    Ok(())
}
