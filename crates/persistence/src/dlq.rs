//! Dead-letter record shape and publish helper (spec section 15). A
//! consumer that cannot correctly process a message — unsupported schema
//! version, malformed envelope, or an inbox payload-hash mismatch
//! (invariant I11's integrity case) — publishes one of these to
//! `<source_topic>.dlq` and only advances its offset ledger once that
//! publish is acknowledged (never before, per section 15: "Publishing to
//! DLQ must be acknowledged before the poison message offset is
//! committed").

use chrono::{DateTime, Utc};
use messaging::{MessagingError, Producer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRecord {
    pub original_topic: String,
    pub original_partition: i32,
    pub original_offset: i64,
    pub original_key: Option<String>,
    /// Best-effort: the raw envelope JSON when it at least parses, so an
    /// operator can inspect it; a completely malformed payload instead
    /// gets a `null` here with the parse failure in `error_detail`.
    pub envelope: Option<Value>,
    pub consumer: String,
    pub attempts: u32,
    pub first_failure_at: DateTime<Utc>,
    pub last_failure_at: DateTime<Utc>,
    pub error_code: String,
    pub error_detail: String,
    pub replay_count: u32,
}

pub fn dlq_topic(source_topic: &str) -> String {
    format!("{source_topic}.dlq")
}

pub async fn publish(
    producer: &dyn Producer,
    source_topic: &str,
    key: &str,
    record: &DlqRecord,
) -> Result<(), MessagingError> {
    let payload = serde_json::to_vec(record).expect("dlq record serializes to json");
    let headers = vec![
        (
            "error_code".to_string(),
            record.error_code.clone().into_bytes(),
        ),
        (
            "original_topic".to_string(),
            record.original_topic.clone().into_bytes(),
        ),
    ];
    producer
        .publish(&dlq_topic(source_topic), key, payload, headers)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dlq_topic_appends_suffix() {
        assert_eq!(
            dlq_topic("inventory.commands.v1"),
            "inventory.commands.v1.dlq"
        );
    }
}
