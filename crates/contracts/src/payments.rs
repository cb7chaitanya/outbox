//! Payments command/event payloads (spec section 8): the `authorize_payment`
//! / `refund_payment` commands orders sends and the
//! `payment_authorized` / `payment_failed` / `payment_refunded` events
//! payments replies with. Both commands share one topic
//! (`payments.commands.v1`); the consumer dispatches on `event_type`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PAYMENTS_COMMANDS_TOPIC: &str = "payments.commands.v1";
pub const PAYMENTS_EVENTS_TOPIC: &str = "payments.events.v1";
pub const PAYMENTS_PRODUCER_NAME: &str = "payments";

pub const AUTHORIZE_PAYMENT_COMMAND_TYPE: &str = "payments.authorize_payment";
pub const AUTHORIZE_PAYMENT_SCHEMA_VERSION: u32 = 1;

pub const PAYMENT_AUTHORIZED_EVENT_TYPE: &str = "payments.payment_authorized";
pub const PAYMENT_AUTHORIZED_SCHEMA_VERSION: u32 = 1;

pub const PAYMENT_FAILED_EVENT_TYPE: &str = "payments.payment_failed";
pub const PAYMENT_FAILED_SCHEMA_VERSION: u32 = 1;

pub const REFUND_PAYMENT_COMMAND_TYPE: &str = "payments.refund_payment";
pub const REFUND_PAYMENT_SCHEMA_VERSION: u32 = 1;

pub const PAYMENT_REFUNDED_EVENT_TYPE: &str = "payments.payment_refunded";
pub const PAYMENT_REFUNDED_SCHEMA_VERSION: u32 = 1;

/// Both commands carry `order_id` as their envelope `aggregate_id`,
/// matching `inventory.reserve_inventory`'s convention (spec section 8):
/// the order aggregate owns the version these commands are ordered against.
pub const PAYMENT_COMMAND_AGGREGATE_TYPE: &str = "order";

/// `payment_authorized`/`payment_failed`/`payment_refunded` are payments'
/// own facts about a payment, keyed by the payment aggregate itself.
pub const PAYMENT_AGGREGATE_TYPE: &str = "payment";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentAmount {
    pub currency: String,
    pub minor_units: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizePaymentPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub amount: PaymentAmount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentAuthorizedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub provider_reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentFailedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub reason_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundPaymentPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentRefundedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Envelope;

    #[test]
    fn authorize_payment_envelope_round_trips() {
        let payload = AuthorizePaymentPayload {
            order_id: Uuid::now_v7(),
            payment_id: Uuid::now_v7(),
            amount: PaymentAmount {
                currency: "USD".to_string(),
                minor_units: 2500,
            },
        };
        let env = Envelope {
            event_id: Uuid::now_v7(),
            event_type: AUTHORIZE_PAYMENT_COMMAND_TYPE.to_string(),
            schema_version: AUTHORIZE_PAYMENT_SCHEMA_VERSION,
            occurred_at: chrono::Utc::now(),
            producer: "orders".to_string(),
            aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
            aggregate_id: payload.order_id,
            aggregate_version: 2,
            correlation_id: Uuid::now_v7(),
            causation_id: Uuid::now_v7(),
            traceparent: None,
            payload,
        };
        let bytes = serde_json::to_vec(&env).unwrap();
        let back: Envelope<AuthorizePaymentPayload> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.payload.order_id, env.payload.order_id);
    }
}
