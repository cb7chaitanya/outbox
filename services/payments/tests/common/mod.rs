//! Shared integration-test scaffolding for the payments consumer (spec
//! section 18): real Postgres (via `#[sqlx::test]`) and real Redpanda,
//! with the offset ledger seeded to the topic's current high watermark
//! before each test publishes — so a test never replays every record any
//! earlier test run left on the shared dev topic.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use chrono::Utc;
use contracts::Envelope;
use contracts::payments::{
    AUTHORIZE_PAYMENT_COMMAND_TYPE, AUTHORIZE_PAYMENT_SCHEMA_VERSION, AuthorizePaymentPayload,
    PAYMENT_COMMAND_AGGREGATE_TYPE, PaymentAmount, REFUND_PAYMENT_COMMAND_TYPE,
    REFUND_PAYMENT_SCHEMA_VERSION, RefundPaymentPayload,
};
use messaging::{Consumer, Producer, RskafkaConsumer, RskafkaProducer};
use payments::consumer::{CONSUMER_NAME, ProcessSummary, RetryConfig, SOURCE_PARTITION};
use payments::provider::PaymentProvider;
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

/// Every test in this binary that reads `payments.commands.v1` (or its
/// DLQ) shares one real, persistent topic on the dev broker — see
/// `inventory`'s identical scaffolding note for why this lock exists.
static TOPIC_LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();

pub async fn topic_lock() -> MutexGuard<'static, ()> {
    TOPIC_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

pub fn broker() -> String {
    std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string())
}

pub async fn connect_producer() -> RskafkaProducer {
    RskafkaProducer::connect(vec![broker()])
        .await
        .expect("connect producer to redpanda")
}

pub async fn connect_consumer() -> RskafkaConsumer {
    RskafkaConsumer::connect(vec![broker()])
        .await
        .expect("connect consumer to redpanda")
}

pub async fn seed_offset_to_latest(pool: &PgPool, consumer: &dyn Consumer, topic: &str) {
    let latest = consumer
        .latest_offset(topic)
        .await
        .expect("read latest offset");
    persistence::inbox::commit_offset(
        pool,
        CONSUMER_NAME,
        topic,
        SOURCE_PARTITION,
        latest,
        Utc::now(),
    )
    .await
    .expect("seed offset ledger");
}

/// Fast retry config for tests: small backoff so a fault-triggered retry
/// resolves in well under a second instead of the production 100ms/30s
/// defaults compounding across `max_attempts`.
pub fn fast_retry_config() -> RetryConfig {
    RetryConfig {
        max_attempts: 5,
        max_elapsed: Duration::from_secs(5),
        backoff_base: Duration::from_millis(5),
        backoff_cap: Duration::from_millis(50),
    }
}

pub fn build_authorize_envelope(
    order_id: Uuid,
    payment_id: Uuid,
    amount_minor: i64,
    currency: &str,
    order_version: i64,
    correlation_id: Uuid,
) -> (Uuid, Vec<u8>) {
    let event_id = Uuid::now_v7();
    let envelope = Envelope {
        event_id,
        event_type: AUTHORIZE_PAYMENT_COMMAND_TYPE.to_string(),
        schema_version: AUTHORIZE_PAYMENT_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        correlation_id,
        causation_id: Uuid::now_v7(),
        traceparent: None,
        payload: AuthorizePaymentPayload {
            order_id,
            payment_id,
            amount: PaymentAmount {
                currency: currency.to_string(),
                minor_units: amount_minor,
            },
        },
    };
    let bytes = serde_json::to_vec(&envelope).expect("envelope serializes");
    (event_id, bytes)
}

pub fn build_refund_envelope(
    order_id: Uuid,
    payment_id: Uuid,
    order_version: i64,
    correlation_id: Uuid,
) -> (Uuid, Vec<u8>) {
    let event_id = Uuid::now_v7();
    let envelope = Envelope {
        event_id,
        event_type: REFUND_PAYMENT_COMMAND_TYPE.to_string(),
        schema_version: REFUND_PAYMENT_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        correlation_id,
        causation_id: Uuid::now_v7(),
        traceparent: None,
        payload: RefundPaymentPayload {
            order_id,
            payment_id,
            reason: "fulfilment_failed".to_string(),
        },
    };
    let bytes = serde_json::to_vec(&envelope).expect("envelope serializes");
    (event_id, bytes)
}

pub async fn publish_raw(producer: &dyn Producer, topic: &str, key: &str, bytes: Vec<u8>) {
    producer
        .publish(topic, key, bytes, vec![])
        .await
        .expect("publish raw payload");
}

/// Generous, centralized, and documented per spec section 18.
pub const DRAIN_BOUND: Duration = Duration::from_secs(10);

pub async fn drain(
    pool: &PgPool,
    consumer: &dyn Consumer,
    producer: &dyn Producer,
    provider: &dyn PaymentProvider,
    fault_injector: &FaultInjector,
    topic: &str,
    retry_config: &RetryConfig,
) -> ProcessSummary {
    let mut total = ProcessSummary::default();
    let deadline = tokio::time::Instant::now() + DRAIN_BOUND;
    loop {
        let summary = payments::consumer::process_available(
            pool,
            consumer,
            producer,
            provider,
            fault_injector,
            topic,
            500,
            retry_config,
        )
        .await
        .expect("process_available");

        total.records_seen += summary.records_seen;
        total.applied += summary.applied;
        total.duplicate += summary.duplicate;
        total.stale += summary.stale;
        total.poison += summary.poison;

        if summary.stopped_by_fault {
            total.stopped_by_fault = true;
            return total;
        }
        if summary.records_seen == 0 {
            return total;
        }
        if tokio::time::Instant::now() > deadline {
            return total;
        }
    }
}

pub fn fake_provider(fault_injector: Arc<FaultInjector>) -> payments::provider::FakeProvider {
    payments::provider::FakeProvider::new(fault_injector)
}
