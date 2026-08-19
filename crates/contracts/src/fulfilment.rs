//! Fulfilment command/event payloads (spec section 8): the
//! `create_fulfilment` command orders sends once both inventory reservation
//! and payment authorization are confirmed, and the `fulfilment_created` /
//! `fulfilment_failed` events fulfilment replies with.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const FULFILMENT_COMMANDS_TOPIC: &str = "fulfilment.commands.v1";
pub const FULFILMENT_EVENTS_TOPIC: &str = "fulfilment.events.v1";
pub const FULFILMENT_PRODUCER_NAME: &str = "fulfilment";

pub const CREATE_FULFILMENT_COMMAND_TYPE: &str = "fulfilment.create_fulfilment";
pub const CREATE_FULFILMENT_SCHEMA_VERSION: u32 = 1;

pub const FULFILMENT_CREATED_EVENT_TYPE: &str = "fulfilment.fulfilment_created";
pub const FULFILMENT_CREATED_SCHEMA_VERSION: u32 = 1;

pub const FULFILMENT_FAILED_EVENT_TYPE: &str = "fulfilment.fulfilment_failed";
pub const FULFILMENT_FAILED_SCHEMA_VERSION: u32 = 1;

/// `create_fulfilment`'s aggregate type is `order`, matching the
/// orders->inventory / orders->payments command convention (spec section
/// 14; see `docs/adr/0011-per-target-command-version-counter.md`): ordered
/// against the per-`(order_id, "fulfilment")` outbound command counter.
pub const CREATE_FULFILMENT_AGGREGATE_TYPE: &str = "order";

/// `fulfilment_created`/`fulfilment_failed` are fulfilment's own facts
/// about a fulfilment, keyed by the fulfilment aggregate itself.
pub const FULFILMENT_AGGREGATE_TYPE: &str = "fulfilment";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFulfilmentPayload {
    pub order_id: Uuid,
    pub reservation_id: Uuid,
    pub payment_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FulfilmentCreatedPayload {
    pub order_id: Uuid,
    pub fulfilment_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FulfilmentFailedPayload {
    pub order_id: Uuid,
    pub fulfilment_id: Uuid,
    pub reason_code: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Envelope;

    #[test]
    fn create_fulfilment_envelope_round_trips() {
        let payload = CreateFulfilmentPayload {
            order_id: Uuid::now_v7(),
            reservation_id: Uuid::now_v7(),
            payment_id: Uuid::now_v7(),
        };
        let env = Envelope {
            event_id: Uuid::now_v7(),
            event_type: CREATE_FULFILMENT_COMMAND_TYPE.to_string(),
            schema_version: CREATE_FULFILMENT_SCHEMA_VERSION,
            occurred_at: chrono::Utc::now(),
            producer: "orders".to_string(),
            aggregate_type: CREATE_FULFILMENT_AGGREGATE_TYPE.to_string(),
            aggregate_id: payload.order_id,
            aggregate_version: 1,
            correlation_id: Uuid::now_v7(),
            causation_id: Uuid::now_v7(),
            traceparent: None,
            payload,
        };
        let bytes = serde_json::to_vec(&env).unwrap();
        let back: Envelope<CreateFulfilmentPayload> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.payload.order_id, env.payload.order_id);
    }
}
