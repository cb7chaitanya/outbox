//! Fulfilment aggregate: status enum (spec section 9).

use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "fulfilment_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FulfilmentStatus {
    Pending,
    Created,
    Failed,
    Cancelled,
}
