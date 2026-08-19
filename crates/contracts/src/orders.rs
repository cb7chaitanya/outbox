//! `orders.order_created` event: the only domain event M02 needs (spec
//! section 8). Later milestones add the rest of the catalog alongside the
//! services that produce/consume them.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const ORDER_CREATED_EVENT_TYPE: &str = "orders.order_created";
pub const ORDER_CREATED_SCHEMA_VERSION: u32 = 1;
pub const ORDER_CREATED_TOPIC: &str = "orders.events.v1";
pub const ORDERS_PRODUCER_NAME: &str = "orders";
pub const ORDER_AGGREGATE_TYPE: &str = "order";

pub const ORDER_COMPLETED_EVENT_TYPE: &str = "orders.order_completed";
pub const ORDER_COMPLETED_SCHEMA_VERSION: u32 = 1;

pub const ORDER_CANCELLED_EVENT_TYPE: &str = "orders.order_cancelled";
pub const ORDER_CANCELLED_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedItem {
    pub sku: String,
    pub quantity: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedAmount {
    pub currency: String,
    pub minor_units: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedPayload {
    pub order_id: Uuid,
    pub items: Vec<OrderCreatedItem>,
    pub amount: OrderCreatedAmount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCompletedPayload {
    pub order_id: Uuid,
    pub fulfilment_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelledPayload {
    pub order_id: Uuid,
    pub reason_code: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Envelope;

    #[test]
    fn order_created_envelope_round_trips() {
        let payload = OrderCreatedPayload {
            order_id: Uuid::now_v7(),
            items: vec![OrderCreatedItem {
                sku: "SKU-1".to_string(),
                quantity: 2,
            }],
            amount: OrderCreatedAmount {
                currency: "USD".to_string(),
                minor_units: 2500,
            },
        };
        let env = Envelope {
            event_id: Uuid::now_v7(),
            event_type: ORDER_CREATED_EVENT_TYPE.to_string(),
            schema_version: ORDER_CREATED_SCHEMA_VERSION,
            occurred_at: chrono::Utc::now(),
            producer: ORDERS_PRODUCER_NAME.to_string(),
            aggregate_type: ORDER_AGGREGATE_TYPE.to_string(),
            aggregate_id: payload.order_id,
            aggregate_version: 1,
            correlation_id: Uuid::now_v7(),
            causation_id: Uuid::now_v7(),
            traceparent: None,
            payload,
        };
        let bytes = serde_json::to_vec(&env).unwrap();
        let back: Envelope<OrderCreatedPayload> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.payload.order_id, env.payload.order_id);
    }
}
