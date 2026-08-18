//! Producer/consumer ports and the Redpanda (rskafka) adapter.
//!
//! M00 only fixes the port shape so services can be built against it.
//! The real adapter, retry/backoff, and DLQ wiring land in M03 (outbox
//! publisher) and M04 (idempotent consumer).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MessagingError {
    #[error("not yet implemented: {0}")]
    NotImplemented(&'static str),
}

/// Publishes a single envelope to a topic, keyed for partition ordering.
#[async_trait::async_trait]
pub trait Producer: Send + Sync {
    async fn publish(&self, topic: &str, key: &str, payload: Vec<u8>)
    -> Result<(), MessagingError>;
}
