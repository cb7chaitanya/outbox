//! Inventory command/event payloads (spec section 8): the `reserve_inventory`
//! command orders sends and the `reservation_succeeded` /
//! `reservation_failed` events inventory replies with.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const INVENTORY_COMMANDS_TOPIC: &str = "inventory.commands.v1";
pub const INVENTORY_EVENTS_TOPIC: &str = "inventory.events.v1";
pub const INVENTORY_PRODUCER_NAME: &str = "inventory";

pub const RESERVE_INVENTORY_COMMAND_TYPE: &str = "inventory.reserve_inventory";
pub const RESERVE_INVENTORY_SCHEMA_VERSION: u32 = 1;

pub const RESERVATION_SUCCEEDED_EVENT_TYPE: &str = "inventory.reservation_succeeded";
pub const RESERVATION_SUCCEEDED_SCHEMA_VERSION: u32 = 1;

pub const RESERVATION_FAILED_EVENT_TYPE: &str = "inventory.reservation_failed";
pub const RESERVATION_FAILED_SCHEMA_VERSION: u32 = 1;

/// The `reserve_inventory` command's aggregate type is `order`, matching
/// the aggregate that owns the version it carries (`expected_order_version`
/// — the order aggregate's version at the moment orders emitted this
/// command, spec section 8's payload table).
pub const RESERVE_INVENTORY_AGGREGATE_TYPE: &str = "order";

/// `reservation_succeeded`/`reservation_failed` are inventory's own facts
/// about a reservation, keyed by the reservation aggregate itself.
pub const RESERVATION_AGGREGATE_TYPE: &str = "inventory_reservation";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReserveInventoryItem {
    pub sku: String,
    pub quantity: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReserveInventoryPayload {
    pub order_id: Uuid,
    pub items: Vec<ReserveInventoryItem>,
    pub expected_order_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservationSucceededPayload {
    pub order_id: Uuid,
    pub reservation_id: Uuid,
    pub items: Vec<ReserveInventoryItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservationFailedPayload {
    pub order_id: Uuid,
    pub reason_code: String,
    pub items: Vec<ReserveInventoryItem>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Envelope;

    #[test]
    fn reserve_inventory_envelope_round_trips() {
        let payload = ReserveInventoryPayload {
            order_id: Uuid::now_v7(),
            items: vec![ReserveInventoryItem {
                sku: "SKU-1".to_string(),
                quantity: 2,
            }],
            expected_order_version: 1,
        };
        let env = Envelope {
            event_id: Uuid::now_v7(),
            event_type: RESERVE_INVENTORY_COMMAND_TYPE.to_string(),
            schema_version: RESERVE_INVENTORY_SCHEMA_VERSION,
            occurred_at: chrono::Utc::now(),
            producer: "orders".to_string(),
            aggregate_type: RESERVE_INVENTORY_AGGREGATE_TYPE.to_string(),
            aggregate_id: payload.order_id,
            aggregate_version: 1,
            correlation_id: Uuid::now_v7(),
            causation_id: Uuid::now_v7(),
            traceparent: None,
            payload,
        };
        let bytes = serde_json::to_vec(&env).unwrap();
        let back: Envelope<ReserveInventoryPayload> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.payload.order_id, env.payload.order_id);
    }
}
