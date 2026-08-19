//! Stock/reservation domain types (spec section 9).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "reservation_status", rename_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum ReservationStatus {
    Active,
    Released,
    Committed,
    Rejected,
}

/// One SKU/quantity line of a reservation request. `quantity` must be
/// positive — enforced by the `reservation_items` check constraint and,
/// before any database round trip, by [`validate_items`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationLine {
    pub sku: String,
    pub quantity: i64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReservationRequestError {
    #[error("reservation request has no items")]
    Empty,
    #[error("quantity for sku {sku} must be positive, got {quantity}")]
    NonPositiveQuantity { sku: String, quantity: i64 },
    #[error("duplicate sku {0} in one reservation request")]
    DuplicateSku(String),
}

/// Validates a raw (sku, quantity) list before any locking/DB work: no
/// duplicate SKUs within one request (the all-or-nothing lock loop assumes
/// one row per SKU), no non-positive quantities, and at least one line.
pub fn validate_items(
    raw: &[(String, i64)],
) -> Result<Vec<ReservationLine>, ReservationRequestError> {
    if raw.is_empty() {
        return Err(ReservationRequestError::Empty);
    }
    let mut seen = std::collections::HashSet::new();
    let mut lines = Vec::with_capacity(raw.len());
    for (sku, quantity) in raw {
        if *quantity <= 0 {
            return Err(ReservationRequestError::NonPositiveQuantity {
                sku: sku.clone(),
                quantity: *quantity,
            });
        }
        if !seen.insert(sku.clone()) {
            return Err(ReservationRequestError::DuplicateSku(sku.clone()));
        }
        lines.push(ReservationLine {
            sku: sku.clone(),
            quantity: *quantity,
        });
    }
    Ok(lines)
}

/// Sorts SKUs ascending so every caller locks `stock` rows in the same
/// order regardless of the order items arrived in the request (spec
/// section 9: "locking stock rows in sorted SKU order to avoid deadlock").
pub fn sorted_skus(lines: &[ReservationLine]) -> Vec<String> {
    let mut skus: Vec<String> = lines.iter().map(|l| l.sku.clone()).collect();
    skus.sort();
    skus
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(pairs: &[(&str, i64)]) -> Vec<(String, i64)> {
        pairs
            .iter()
            .map(|(sku, qty)| (sku.to_string(), *qty))
            .collect()
    }

    #[test]
    fn rejects_empty_request() {
        assert_eq!(validate_items(&[]), Err(ReservationRequestError::Empty));
    }

    #[test]
    fn rejects_non_positive_quantity() {
        let raw = lines(&[("SKU-1", 0)]);
        assert_eq!(
            validate_items(&raw),
            Err(ReservationRequestError::NonPositiveQuantity {
                sku: "SKU-1".to_string(),
                quantity: 0
            })
        );
    }

    #[test]
    fn rejects_duplicate_sku_in_one_request() {
        let raw = lines(&[("SKU-1", 1), ("SKU-1", 2)]);
        assert_eq!(
            validate_items(&raw),
            Err(ReservationRequestError::DuplicateSku("SKU-1".to_string()))
        );
    }

    #[test]
    fn sorted_skus_is_deterministic_regardless_of_input_order() {
        let raw = lines(&[("SKU-B", 1), ("SKU-A", 2)]);
        let valid = validate_items(&raw).unwrap();
        assert_eq!(sorted_skus(&valid), vec!["SKU-A", "SKU-B"]);
    }
}
