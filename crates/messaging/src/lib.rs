//! Producer/consumer ports and the Redpanda (rskafka) adapter.
//!
//! M02 adds a real, minimal producer for the naive dual-write path (spec
//! section 11): one partition client per topic, no batching, no retry/DLQ
//! handling. The claim-lease outbox publisher with retries and DLQ wiring
//! (spec section 13) lands in M03; the consumer side lands in M04.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use thiserror::Error;
use tokio::sync::Mutex;

pub mod consumer;
pub use consumer::{ConsumedRecord, Consumer, NoopConsumer, RskafkaConsumer};

#[derive(Debug, Error)]
pub enum MessagingError {
    #[error("not yet implemented: {0}")]
    NotImplemented(&'static str),
    #[error("kafka client error: {0}")]
    Client(#[from] rskafka::client::error::Error),
}

/// Publishes a single envelope to a topic, keyed for partition ordering.
/// `headers` carries the `event_id`, correlation/causation, and producer
/// name headers required by spec section 13 point 3; callers that don't
/// need them (e.g. the naive path) may pass an empty vec.
#[async_trait::async_trait]
pub trait Producer: Send + Sync {
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> Result<(), MessagingError>;
}

/// Minimal rskafka-backed [`Producer`]. Every topic this project uses (spec
/// section 8) is created with a single partition, so partition `0` is
/// always correct; a multi-partition topic would need key-hash routing,
/// which is out of scope until a milestone actually needs throughput
/// beyond a single partition's ordering guarantee.
pub struct RskafkaProducer {
    client: Client,
    partition_clients: Mutex<HashMap<String, Arc<PartitionClient>>>,
}

impl RskafkaProducer {
    pub async fn connect(bootstrap_brokers: Vec<String>) -> Result<Self, MessagingError> {
        let client = ClientBuilder::new(bootstrap_brokers).build().await?;
        Ok(Self {
            client,
            partition_clients: Mutex::new(HashMap::new()),
        })
    }

    async fn partition_client(&self, topic: &str) -> Result<Arc<PartitionClient>, MessagingError> {
        let mut guard = self.partition_clients.lock().await;
        if let Some(existing) = guard.get(topic) {
            return Ok(Arc::clone(existing));
        }
        let pc = self
            .client
            .partition_client(topic, 0, UnknownTopicHandling::Error)
            .await?;
        let pc = Arc::new(pc);
        guard.insert(topic.to_string(), Arc::clone(&pc));
        Ok(pc)
    }
}

/// A [`Producer`] that always succeeds without touching a broker, for tests
/// that don't care about the messaging side effect.
#[derive(Debug, Default)]
pub struct NoopProducer;

#[async_trait::async_trait]
impl Producer for NoopProducer {
    async fn publish(
        &self,
        _topic: &str,
        _key: &str,
        _payload: Vec<u8>,
        _headers: Vec<(String, Vec<u8>)>,
    ) -> Result<(), MessagingError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Producer for RskafkaProducer {
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> Result<(), MessagingError> {
        let partition_client = self.partition_client(topic).await?;
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload),
            headers: headers.into_iter().collect(),
            timestamp: Utc::now(),
        };
        partition_client
            .produce(vec![record], Compression::NoCompression)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// Ignored by default: exercises a real Redpanda broker on
    /// `REDPANDA_BROKER` (defaults to the local Compose port). Run with
    /// `cargo test -p messaging -- --ignored` while `make up` is running.
    #[tokio::test]
    #[ignore]
    async fn publishes_to_a_real_topic() {
        let broker =
            std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
        let producer = RskafkaProducer::connect(vec![broker])
            .await
            .expect("connect to redpanda");
        producer
            .publish(
                "orders.events.v1",
                "smoke-test-key",
                b"hello".to_vec(),
                vec![],
            )
            .await
            .expect("publish succeeds");
    }
}
