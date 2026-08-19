//! Shared integration-test scaffolding (spec section 18): builds the
//! `AppState` the router needs, since M02 wired a real producer and fault
//! injector into it.
//!
//! Each `tests/*.rs` file is its own compiled test binary and only uses a
//! subset of these helpers, so per-binary dead-code warnings here are
//! expected, not a real problem.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use contracts::Envelope;
use orders::config::DeliveryMode;
use orders::http::AppState;
use persistence::outbox::PublishMetrics;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

pub fn broker() -> String {
    std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string())
}

pub async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Generous, centralized, and documented per spec section 18: "any
/// unavoidable timing bound is generous, centralized, and documented."
pub const POLL_BOUND: Duration = Duration::from_millis(1500);
pub const POLL_INTERVAL: Duration = Duration::from_millis(150);

/// Reads every record currently on `topic` (from `Earliest` to the current
/// high watermark) whose `aggregate_id` matches, deserialized as
/// `Envelope<T>`. A bounded, single-shot fetch against a locally-owned dev
/// topic; callers wrap it in [`poll_until`] rather than looping internally,
/// so the timing bound lives in one place.
pub async fn matching_records<T: DeserializeOwned>(
    topic: &str,
    aggregate_id: Uuid,
) -> Vec<Envelope<T>> {
    let client = ClientBuilder::new(vec![broker()])
        .build()
        .await
        .expect("connect to redpanda");
    let partition = client
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await
        .expect("partition client");
    let earliest = partition
        .get_offset(OffsetAt::Earliest)
        .await
        .expect("earliest offset");
    let (records, _high_watermark) = partition
        .fetch_records(earliest, 0..50_000_000, 1_000)
        .await
        .expect("fetch records");

    records
        .into_iter()
        .filter_map(|r| {
            let value = r.record.value?;
            let envelope: Envelope<T> = serde_json::from_slice(&value).ok()?;
            (envelope.aggregate_id == aggregate_id).then_some(envelope)
        })
        .collect()
}

/// Polls [`matching_records`] until at least `min_count` matching records
/// are found or [`POLL_BOUND`] elapses, returning whatever was last seen
/// either way (so a negative assertion — "still empty after the bound" —
/// can use the same helper as a positive one).
pub async fn poll_until<T: DeserializeOwned>(
    topic: &str,
    aggregate_id: Uuid,
    min_count: usize,
) -> Vec<Envelope<T>> {
    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + POLL_BOUND;
    while tokio::time::Instant::now() < deadline {
        records = matching_records(topic, aggregate_id).await;
        if records.len() >= min_count {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    records
}

/// Router state for tests that don't care about the Kafka side effect: a
/// `NoopProducer` and an unconfigured fault injector, so `publish_naive`
/// always succeeds silently.
pub fn noop_state(pool: PgPool) -> AppState {
    AppState {
        pool,
        producer: Arc::new(messaging::NoopProducer),
        fault_injector: Arc::new(FaultInjector::new()),
        delivery_mode: DeliveryMode::Naive,
        failure_injection_enabled: false,
        failure_injection_token: String::new(),
        publish_metrics: Arc::new(PublishMetrics::default()),
    }
}

/// Router state for the dual-write demonstration tests: a real
/// `RskafkaProducer` against the broker named by `REDPANDA_BROKER` (falls
/// back to the local Compose port), with failure injection enabled and a
/// fixed test token so `/_test/faults/*` is reachable.
pub const TEST_TOKEN: &str = "integration-test-token";

pub async fn live_state(pool: PgPool) -> AppState {
    live_state_with_mode(pool, DeliveryMode::Naive).await
}

/// Same as [`live_state`] but with an explicit delivery mode, for the
/// outbox-mode tests (spec M03) that need `DeliveryMode::Outbox` instead of
/// the M02-era default.
pub async fn live_state_with_mode(pool: PgPool, delivery_mode: DeliveryMode) -> AppState {
    let broker = std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
    let producer = messaging::RskafkaProducer::connect(vec![broker])
        .await
        .expect("connect to redpanda for integration test");
    AppState {
        pool,
        producer: Arc::new(producer),
        fault_injector: Arc::new(FaultInjector::new()),
        delivery_mode,
        failure_injection_enabled: true,
        failure_injection_token: TEST_TOKEN.to_string(),
        publish_metrics: Arc::new(PublishMetrics::default()),
    }
}
