//! Payment status enum (spec section 9).

use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "payment_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaymentStatus {
    Pending,
    Authorized,
    Failed,
    RefundPending,
    Refunded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(
    type_name = "payment_operation_type",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationType {
    Authorize,
    Refund,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(
    type_name = "payment_operation_status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationStatus {
    Succeeded,
    Failed,
}

/// Idempotency keys handed to the fake provider (spec section 9): stable
/// per order regardless of how many times a command is redelivered or
/// retried, so the provider's own ledger de-duplicates correctly.
pub fn authorize_idempotency_key(order_id: uuid::Uuid) -> String {
    format!("authorize:{order_id}")
}

pub fn refund_idempotency_key(order_id: uuid::Uuid) -> String {
    format!("refund:{order_id}")
}
