//! Consumer port and the rskafka-backed adapter (spec section 14).
//!
//! `rskafka` has no broker-managed consumer-group / offset-commit
//! protocol, unlike `rdkafka`'s high-level consumer (the alternative
//! ruled out in `docs/adr/0001-tech-stack-choices.md` for lacking a
//! cmake/libsasl2-free build in this sandbox). [`Consumer::fetch`] is a
//! plain offset-addressed read: callers own tracking "what's been
//! processed" themselves — see `persistence::inbox::fetch_offset` /
//! `commit_offset` and `docs/adr/0006-inbox-consumer-offset-ledger.md`.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use tokio::sync::Mutex;

use crate::MessagingError;

#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: HashMap<String, Vec<u8>>,
}

/// Every topic this project uses has a single partition (spec section 8),
/// so `fetch` always reads partition `0`.
#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    async fn fetch(
        &self,
        topic: &str,
        offset: i64,
        max_wait_ms: i32,
    ) -> Result<Vec<ConsumedRecord>, MessagingError>;
}

pub struct RskafkaConsumer {
    client: Client,
    partition_clients: Mutex<HashMap<String, Arc<PartitionClient>>>,
}

impl RskafkaConsumer {
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

/// 1 MiB max fetch size per batch — generous for this project's small
/// dev-scale envelopes, well under Redpanda's default broker message-size
/// limit.
const FETCH_BYTE_RANGE: Range<i32> = 1..1_048_576;

#[async_trait::async_trait]
impl Consumer for RskafkaConsumer {
    async fn fetch(
        &self,
        topic: &str,
        offset: i64,
        max_wait_ms: i32,
    ) -> Result<Vec<ConsumedRecord>, MessagingError> {
        let partition_client = self.partition_client(topic).await?;
        let (records, _high_watermark) = partition_client
            .fetch_records(offset, FETCH_BYTE_RANGE, max_wait_ms)
            .await?;
        Ok(records
            .into_iter()
            .map(|r| ConsumedRecord {
                offset: r.offset,
                key: r.record.key,
                value: r.record.value,
                headers: r.record.headers.into_iter().collect(),
            })
            .collect())
    }
}

/// A [`Consumer`] that always returns no records, for tests that don't
/// exercise the fetch loop itself.
#[derive(Debug, Default)]
pub struct NoopConsumer;

#[async_trait::async_trait]
impl Consumer for NoopConsumer {
    async fn fetch(
        &self,
        _topic: &str,
        _offset: i64,
        _max_wait_ms: i32,
    ) -> Result<Vec<ConsumedRecord>, MessagingError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;
    use crate::{Producer, RskafkaProducer};

    /// Ignored by default: exercises a real Redpanda broker. Run with
    /// `cargo test -p messaging -- --ignored` while `make up` is running.
    #[tokio::test]
    #[ignore]
    async fn fetches_what_was_produced() {
        let broker =
            std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
        let producer = RskafkaProducer::connect(vec![broker.clone()])
            .await
            .expect("connect producer");
        let consumer = RskafkaConsumer::connect(vec![broker])
            .await
            .expect("connect consumer");

        let topic = "orders.events.v1";
        let before = consumer
            .fetch(topic, 0, 100)
            .await
            .expect("initial fetch");
        let start_offset = before.last().map(|r| r.offset + 1).unwrap_or(0);

        producer
            .publish(topic, "consumer-smoke-key", b"hello-consumer".to_vec(), vec![])
            .await
            .expect("publish");

        let after = consumer
            .fetch(topic, start_offset, 5_000)
            .await
            .expect("fetch after publish");
        assert!(!after.is_empty(), "expected at least one new record");
        assert_eq!(after[0].value.as_deref(), Some(b"hello-consumer".as_slice()));
    }
}
