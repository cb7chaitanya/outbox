//! Canonical event envelope shape (see PROJECT_2_SPEC.md section 8).
//!
//! Payload types, validation rules, and the domain event/command catalog
//! land with the milestones that first produce or consume them (M02+).
//! This crate only fixes the envelope shape so every producer/consumer
//! agrees on it from the start.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at: DateTime<Utc>,
    pub producer: String,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub aggregate_version: i64,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    pub payload: P,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn envelope_round_trips_through_json() {
        let env = Envelope {
            event_id: Uuid::now_v7(),
            event_type: "orders.order_created".to_string(),
            schema_version: 1,
            occurred_at: Utc::now(),
            producer: "orders".to_string(),
            aggregate_type: "order".to_string(),
            aggregate_id: Uuid::now_v7(),
            aggregate_version: 1,
            correlation_id: Uuid::now_v7(),
            causation_id: Uuid::now_v7(),
            traceparent: None,
            payload: json!({"order_id": "placeholder"}),
        };
        let serialized = serde_json::to_string(&env).unwrap();
        let deserialized: Envelope<serde_json::Value> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.event_type, "orders.order_created");
    }
}
