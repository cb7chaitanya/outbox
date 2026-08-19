//! Postgres-backed persistence for stock and reservations.
//!
//! `reserve` implements the all-or-nothing, deadlock-avoiding locking
//! order from spec section 9: every SKU touched by one reservation is
//! locked with `SELECT ... FOR UPDATE` in sorted order inside a single
//! transaction, so two concurrent reservations that share a SKU serialize
//! on that row rather than deadlocking or overselling (invariant I5), and
//! a reservation either reserves every requested SKU or none of them.
//! `order_id` is unique on `reservations`, so a redelivered request for an
//! already-decided order replays the original outcome instead of
//! re-locking stock (invariant I6).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::domain::{ReservationLine, ReservationStatus, sorted_skus};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StockRow {
    pub sku: String,
    pub available_qty: i64,
    pub reserved_qty: i64,
    pub fulfilled_qty: i64,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReservationRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub status: ReservationStatus,
    pub reason_code: Option<String>,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Dev/test-only: creates or resets a SKU's stock level (spec section 6:
/// inventory's synchronous API includes "dev-only stock seed/read").
pub async fn seed_stock(
    pool: &PgPool,
    sku: &str,
    available_qty: i64,
    now: DateTime<Utc>,
) -> Result<StockRow, sqlx::Error> {
    sqlx::query_as::<_, StockRow>(
        "insert into stock (sku, available_qty, reserved_qty, fulfilled_qty, version, created_at, updated_at) \
         values ($1, $2, 0, 0, 1, $3, $3) \
         on conflict (sku) do update set available_qty = excluded.available_qty, updated_at = excluded.updated_at \
         returning sku, available_qty, reserved_qty, fulfilled_qty, version, created_at, updated_at",
    )
    .bind(sku)
    .bind(available_qty)
    .bind(now)
    .fetch_one(pool)
    .await
}

pub async fn get_stock(pool: &PgPool, sku: &str) -> Result<Option<StockRow>, sqlx::Error> {
    sqlx::query_as::<_, StockRow>(
        "select sku, available_qty, reserved_qty, fulfilled_qty, version, created_at, updated_at \
         from stock where sku = $1",
    )
    .bind(sku)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Clone)]
pub struct ReserveOutcome {
    pub reservation_id: Uuid,
    pub status: ReservationStatus,
    pub reason_code: Option<String>,
    /// `true` if this call decided the outcome; `false` if it replayed an
    /// already-decided reservation for this `order_id`.
    pub created: bool,
}

/// Attempts to reserve every line item for `order_id` atomically. Returns
/// `Active` with no `reason_code` on success, or `Rejected` with a reason
/// when any SKU lacks sufficient stock — never a partial reservation.
///
/// Takes an already-open transaction connection rather than a pool and
/// beginning/committing its own transaction: the consumer handler
/// (`consumer::handle_one`) must apply this mutation and its resulting
/// outbox event, inbox mark, and consumer-version advance all in one
/// transaction (spec section 14 steps 4-6), so this function cannot own
/// the transaction boundary itself.
pub async fn reserve(
    conn: &mut PgConnection,
    order_id: Uuid,
    lines: &[ReservationLine],
    now: DateTime<Utc>,
) -> Result<ReserveOutcome, sqlx::Error> {
    if let Some(existing) = sqlx::query_as::<_, ReservationRow>(
        "select id, order_id, status, reason_code, version, created_at, updated_at \
         from reservations where order_id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(ReserveOutcome {
            reservation_id: existing.id,
            status: existing.status,
            reason_code: existing.reason_code,
            created: false,
        });
    }

    let skus = sorted_skus(lines);
    let stock_rows = sqlx::query_as::<_, StockRow>(
        "select sku, available_qty, reserved_qty, fulfilled_qty, version, created_at, updated_at \
         from stock where sku = any($1) order by sku for update",
    )
    .bind(&skus)
    .fetch_all(&mut *conn)
    .await?;
    let stock_by_sku: HashMap<&str, &StockRow> =
        stock_rows.iter().map(|r| (r.sku.as_str(), r)).collect();

    let mut rejection_reason: Option<&'static str> = None;
    for line in lines {
        match stock_by_sku.get(line.sku.as_str()) {
            None => rejection_reason.get_or_insert("SKU_NOT_FOUND"),
            Some(row) if row.available_qty < line.quantity => {
                rejection_reason.get_or_insert("INSUFFICIENT_STOCK")
            }
            Some(_) => continue,
        };
    }

    let reservation_id = Uuid::now_v7();

    if let Some(reason) = rejection_reason {
        sqlx::query(
            "insert into reservations (id, order_id, status, reason_code, version, created_at, updated_at) \
             values ($1, $2, 'REJECTED', $3, 1, $4, $4)",
        )
        .bind(reservation_id)
        .bind(order_id)
        .bind(reason)
        .bind(now)
        .execute(&mut *conn)
        .await?;
        return Ok(ReserveOutcome {
            reservation_id,
            status: ReservationStatus::Rejected,
            reason_code: Some(reason.to_string()),
            created: true,
        });
    }

    for line in lines {
        sqlx::query(
            "update stock set available_qty = available_qty - $1, reserved_qty = reserved_qty + $1, \
             version = version + 1, updated_at = $3 where sku = $2",
        )
        .bind(line.quantity)
        .bind(&line.sku)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }

    sqlx::query(
        "insert into reservations (id, order_id, status, version, created_at, updated_at) \
         values ($1, $2, 'ACTIVE', 1, $3, $3)",
    )
    .bind(reservation_id)
    .bind(order_id)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    for line in lines {
        sqlx::query(
            "insert into reservation_items (reservation_id, sku, quantity) values ($1, $2, $3)",
        )
        .bind(reservation_id)
        .bind(&line.sku)
        .bind(line.quantity)
        .execute(&mut *conn)
        .await?;
    }

    Ok(ReserveOutcome {
        reservation_id,
        status: ReservationStatus::Active,
        reason_code: None,
        created: true,
    })
}

#[derive(Debug, Clone)]
pub struct ReleaseOutcome {
    pub reservation_id: Uuid,
    /// `true` only when this call actually released stock (the reservation
    /// was `ACTIVE`); `false` for a redelivered/duplicate release request
    /// against an already-`RELEASED` reservation, or a defensive no-op
    /// against a `REJECTED`/`COMMITTED` one (neither has active stock to
    /// give back — `REJECTED` reservations are compensation matrix row 1's
    /// job, which never emits a release command in the first place, and
    /// `COMMITTED` is unreachable before fulfilment exists in M07). The
    /// caller uses this to decide whether to emit `inventory_released`
    /// (spec section 12: "Releasing an already released reservation...
    /// return[s] logical success without repeating the effect").
    pub created: bool,
}
#[derive(Debug, thiserror::Error)]
pub enum ReleaseError {
    #[error("no reservation found for order {0}")]
    NotFound(Uuid),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// Releases every SKU held by `order_id`'s reservation, restoring
/// `available_qty`/`reserved_qty` (invariant I5), locking stock rows in the
/// same sorted-SKU order as [`reserve`] to avoid deadlocking against a
/// concurrent reservation on an overlapping SKU set.
///
/// Takes an already-open transaction connection for the same reason
/// [`reserve`] does: the caller applies this mutation and its resulting
/// outbox event, inbox mark, and consumer-version advance in one
/// transaction (spec section 14 steps 4-6).
pub async fn release(
    conn: &mut PgConnection,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<ReleaseOutcome, ReleaseError> {
    let reservation = sqlx::query_as::<_, ReservationRow>(
        "select id, order_id, status, reason_code, version, created_at, updated_at \
         from reservations where order_id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or(ReleaseError::NotFound(order_id))?;

    if reservation.status != ReservationStatus::Active {
        // Idempotent replay (already RELEASED) or a defensive no-op
        // (REJECTED/COMMITTED never had stock reserved to give back).
        return Ok(ReleaseOutcome {
            reservation_id: reservation.id,
            created: false,
        });
    }

    let items = sqlx::query_as::<_, (String, i64)>(
        "select sku, quantity from reservation_items where reservation_id = $1 order by sku",
    )
    .bind(reservation.id)
    .fetch_all(&mut *conn)
    .await?;

    for (sku, quantity) in &items {
        sqlx::query(
            "update stock set available_qty = available_qty + $1, reserved_qty = reserved_qty - $1, \
             version = version + 1, updated_at = $3 where sku = $2",
        )
        .bind(quantity)
        .bind(sku)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }

    sqlx::query(
        "update reservations set status = 'RELEASED', version = version + 1, updated_at = $2 \
         where id = $1",
    )
    .bind(reservation.id)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(ReleaseOutcome {
        reservation_id: reservation.id,
        created: true,
    })
}
