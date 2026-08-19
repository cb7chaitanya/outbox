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
use messaging::{Consumer, Producer, RskafkaConsumer, RskafkaProducer};
use orders::config::DeliveryMode;
use orders::http::AppState;
use orders::outcome_consumer::ProcessSummary;
use persistence::outbox::PublishMetrics;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

/// Every test in this binary that publishes to and drains a shared,
/// persistent dev topic (rather than only reading records filtered by a
/// fresh per-test aggregate id) must hold this lock for its full body —
/// same reasoning as `inventory`'s test harness.
static TOPIC_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

pub async fn topic_lock() -> tokio::sync::MutexGuard<'static, ()> {
    TOPIC_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// Publishes a raw envelope (already-serialized bytes) with no headers, for
/// tests that construct the exact bytes another service would have
/// produced rather than going through this project's own `Producer::publish`
/// call sites.
pub async fn publish_raw(producer: &dyn Producer, topic: &str, key: &str, bytes: Vec<u8>) {
    producer
        .publish(topic, key, bytes, vec![])
        .await
        .expect("publish raw envelope");
}

pub async fn connect_consumer() -> RskafkaConsumer {
    RskafkaConsumer::connect(vec![broker()])
        .await
        .expect("connect consumer to redpanda")
}

pub async fn connect_producer() -> RskafkaProducer {
    RskafkaProducer::connect(vec![broker()])
        .await
        .expect("connect producer to redpanda")
}

/// Seeds `orders::outcome_consumer`'s offset ledger for `topic` to the
/// topic's current high watermark, so a test starts from "now" instead of
/// replaying every record any earlier test run left on the shared dev
/// topic (same reasoning as `inventory`'s test harness).
pub async fn seed_outcome_offset_to_latest(pool: &PgPool, consumer: &dyn Consumer, topic: &str) {
    let latest = consumer
        .latest_offset(topic)
        .await
        .expect("read latest offset");
    persistence::inbox::commit_offset(
        pool,
        orders::outcome_consumer::CONSUMER_NAME,
        topic,
        orders::outcome_consumer::SOURCE_PARTITION,
        latest,
        chrono::Utc::now(),
    )
    .await
    .expect("seed offset ledger");
}

/// Generous, centralized bound for draining the outcome consumer (spec
/// section 18).
pub const OUTCOME_DRAIN_BOUND: Duration = Duration::from_secs(5);

/// Calls `outcome_consumer::process_available` repeatedly until a fetch
/// comes back empty, the fault point stops the batch, or the bound elapses.
pub async fn drain_outcomes(
    pool: &PgPool,
    consumer: &dyn Consumer,
    producer: &dyn Producer,
    fault_injector: &FaultInjector,
    topic: &str,
) -> ProcessSummary {
    let mut total = ProcessSummary::default();
    let deadline = tokio::time::Instant::now() + OUTCOME_DRAIN_BOUND;
    loop {
        let summary = orders::outcome_consumer::process_available(
            pool,
            consumer,
            producer,
            fault_injector,
            topic,
            500,
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

/// Publishes whatever is currently sitting unpublished in this test's own
/// `outbox_events` table — nothing else drains it onto the real broker
/// unless the test runs the real `orders` binary's publisher loop.
pub async fn drain_outbox(pool: &PgPool, producer: &dyn Producer) {
    let fault_injector = FaultInjector::new();
    let metrics = PublishMetrics::default();
    loop {
        let claimed = persistence::outbox::run_publisher_once(
            pool,
            producer,
            &fault_injector,
            &persistence::outbox::PublisherConfig::default(),
            &metrics,
            chrono::Utc::now(),
        )
        .await
        .expect("run_publisher_once");
        if claimed == 0 {
            return;
        }
    }
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
