//! Order aggregate: status enum, the legal transition graph (spec section
//! 12, invariant I2), and money/item validation (spec section 10).
//!
//! Only `PENDING` is reachable end-to-end in M01 — no downstream service
//! exists yet to drive the rest of the graph — but the full graph is
//! encoded now so later milestones extend behavior, not the state machine
//! itself.

use std::fmt;

use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "order_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Pending,
    InventoryReserved,
    PaymentAuthorized,
    ReadyForFulfilment,
    Completed,
    Cancelling,
    Cancelled,
    ManualReview,
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            OrderStatus::Pending => "PENDING",
            OrderStatus::InventoryReserved => "INVENTORY_RESERVED",
            OrderStatus::PaymentAuthorized => "PAYMENT_AUTHORIZED",
            OrderStatus::ReadyForFulfilment => "READY_FOR_FULFILMENT",
            OrderStatus::Completed => "COMPLETED",
            OrderStatus::Cancelling => "CANCELLING",
            OrderStatus::Cancelled => "CANCELLED",
            OrderStatus::ManualReview => "MANUAL_REVIEW",
        };
        write!(f, "{s}")
    }
}

/// Invariant I2: an order status changes only along this graph.
///
/// ```text
/// PENDING -> INVENTORY_RESERVED -> PAYMENT_AUTHORIZED -> READY_FOR_FULFILMENT -> COMPLETED
/// PENDING | INVENTORY_RESERVED | PAYMENT_AUTHORIZED | READY_FOR_FULFILMENT -> CANCELLING
/// CANCELLING -> CANCELLED | MANUAL_REVIEW
/// ```
pub fn is_legal_transition(from: OrderStatus, to: OrderStatus) -> bool {
    use OrderStatus::*;
    matches!(
        (from, to),
        (Pending, InventoryReserved)
            | (InventoryReserved, PaymentAuthorized)
            | (PaymentAuthorized, ReadyForFulfilment)
            | (ReadyForFulfilment, Completed)
            | (Pending, Cancelling)
            | (InventoryReserved, Cancelling)
            | (PaymentAuthorized, Cancelling)
            | (ReadyForFulfilment, Cancelling)
            | (Cancelling, Cancelled)
            | (Cancelling, ManualReview)
    )
}

pub const MAX_ITEMS: usize = 100;
pub const MAX_QUANTITY: i64 = 10_000;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("items must contain between 1 and {MAX_ITEMS} entries")]
    ItemCountOutOfRange,
    #[error("duplicate sku in items: {0}")]
    DuplicateSku(String),
    #[error("quantity for sku {sku} must be between 1 and {MAX_QUANTITY}, got {quantity}")]
    QuantityOutOfRange { sku: String, quantity: i64 },
    #[error("unit_price_minor for sku {0} must be non-negative")]
    NegativeUnitPrice(String),
    #[error("currency must be an uppercase 3-letter ISO-4217-shaped code, got {0:?}")]
    InvalidCurrency(String),
    #[error("computed order amount overflowed i64")]
    AmountOverflow,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ItemRequest {
    pub sku: String,
    pub quantity: i64,
    pub unit_price_minor: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateOrderRequest {
    pub items: Vec<ItemRequest>,
    pub currency: String,
}

/// A validated, normalized order request: items sorted by SKU so that two
/// requests differing only in item order still hash identically for
/// idempotency comparison, and the server-computed total in minor units.
#[derive(Debug, Clone)]
pub struct NormalizedOrder {
    pub items: Vec<ItemRequest>,
    pub currency: String,
    pub amount_minor: i64,
}

pub fn validate_and_normalize(req: CreateOrderRequest) -> Result<NormalizedOrder, ValidationError> {
    if req.items.is_empty() || req.items.len() > MAX_ITEMS {
        return Err(ValidationError::ItemCountOutOfRange);
    }

    let currency_bytes = req.currency.as_bytes();
    if currency_bytes.len() != 3 || !currency_bytes.iter().all(u8::is_ascii_uppercase) {
        return Err(ValidationError::InvalidCurrency(req.currency));
    }

    let mut items = req.items;
    items.sort_by(|a, b| a.sku.cmp(&b.sku));

    for window in items.windows(2) {
        if window[0].sku == window[1].sku {
            return Err(ValidationError::DuplicateSku(window[0].sku.clone()));
        }
    }

    let mut amount_minor: i64 = 0;
    for item in &items {
        if item.quantity < 1 || item.quantity > MAX_QUANTITY {
            return Err(ValidationError::QuantityOutOfRange {
                sku: item.sku.clone(),
                quantity: item.quantity,
            });
        }
        if item.unit_price_minor < 0 {
            return Err(ValidationError::NegativeUnitPrice(item.sku.clone()));
        }
        let line_total = item
            .quantity
            .checked_mul(item.unit_price_minor)
            .ok_or(ValidationError::AmountOverflow)?;
        amount_minor = amount_minor
            .checked_add(line_total)
            .ok_or(ValidationError::AmountOverflow)?;
    }

    Ok(NormalizedOrder {
        items,
        currency: req.currency,
        amount_minor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(items: Vec<(&str, i64, i64)>, currency: &str) -> CreateOrderRequest {
        CreateOrderRequest {
            items: items
                .into_iter()
                .map(|(sku, quantity, unit_price_minor)| ItemRequest {
                    sku: sku.to_string(),
                    quantity,
                    unit_price_minor,
                })
                .collect(),
            currency: currency.to_string(),
        }
    }

    #[test]
    fn computes_total_from_items() {
        let n = validate_and_normalize(req(vec![("SKU-1", 2, 1250), ("SKU-2", 1, 500)], "USD"))
            .unwrap();
        assert_eq!(n.amount_minor, 3000);
    }

    #[test]
    fn rejects_empty_items() {
        assert_eq!(
            validate_and_normalize(req(vec![], "USD")).unwrap_err(),
            ValidationError::ItemCountOutOfRange
        );
    }

    #[test]
    fn rejects_too_many_items() {
        // 101 distinct SKUs so the item-count check trips, not the
        // duplicate-SKU check.
        let items: Vec<ItemRequest> = (0..101)
            .map(|i| ItemRequest {
                sku: format!("SKU-{i}"),
                quantity: 1,
                unit_price_minor: 100,
            })
            .collect();
        let req = CreateOrderRequest {
            items,
            currency: "USD".to_string(),
        };
        assert_eq!(
            validate_and_normalize(req).unwrap_err(),
            ValidationError::ItemCountOutOfRange
        );
    }

    #[test]
    fn rejects_zero_and_negative_quantity() {
        assert!(matches!(
            validate_and_normalize(req(vec![("SKU-1", 0, 100)], "USD")),
            Err(ValidationError::QuantityOutOfRange { .. })
        ));
        assert!(matches!(
            validate_and_normalize(req(vec![("SKU-1", -1, 100)], "USD")),
            Err(ValidationError::QuantityOutOfRange { .. })
        ));
    }

    #[test]
    fn rejects_quantity_over_max() {
        assert!(matches!(
            validate_and_normalize(req(vec![("SKU-1", MAX_QUANTITY + 1, 100)], "USD")),
            Err(ValidationError::QuantityOutOfRange { .. })
        ));
    }

    #[test]
    fn rejects_lowercase_currency() {
        assert!(matches!(
            validate_and_normalize(req(vec![("SKU-1", 1, 100)], "usd")),
            Err(ValidationError::InvalidCurrency(_))
        ));
    }

    #[test]
    fn rejects_duplicate_sku() {
        assert!(matches!(
            validate_and_normalize(req(vec![("SKU-1", 1, 100), ("SKU-1", 2, 100)], "USD")),
            Err(ValidationError::DuplicateSku(_))
        ));
    }

    #[test]
    fn rejects_amount_overflow() {
        assert!(matches!(
            validate_and_normalize(req(vec![("SKU-1", MAX_QUANTITY, i64::MAX)], "USD")),
            Err(ValidationError::AmountOverflow)
        ));
    }

    #[test]
    fn legal_transition_graph_matches_spec() {
        use OrderStatus::*;
        assert!(is_legal_transition(Pending, InventoryReserved));
        assert!(is_legal_transition(InventoryReserved, PaymentAuthorized));
        assert!(is_legal_transition(PaymentAuthorized, ReadyForFulfilment));
        assert!(is_legal_transition(ReadyForFulfilment, Completed));
        assert!(is_legal_transition(Pending, Cancelling));
        assert!(is_legal_transition(Cancelling, Cancelled));
        assert!(is_legal_transition(Cancelling, ManualReview));

        assert!(!is_legal_transition(Pending, Completed));
        assert!(!is_legal_transition(Pending, PaymentAuthorized));
        assert!(!is_legal_transition(Cancelled, Pending));
        assert!(!is_legal_transition(Completed, Cancelling));
        assert!(!is_legal_transition(ManualReview, Cancelled));
    }
}
