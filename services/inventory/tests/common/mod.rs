//! Shared integration-test scaffolding for the reserve_inventory consumer
//! (spec section 18): real Postgres (via `#[sqlx::test]`) and real
//! Redpanda, with the offset ledger seeded to the topic's *current* high
//! watermark before each test publishes — so a test never replays every
//! record any earlier test run left on the shared dev topic.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use chrono::Utc;
use contracts::Envelope;
use contracts::inventory::{
    RESERVE_INVENTORY_AGGREGATE_TYPE, RESERVE_INVENTORY_COMMAND_TYPE,
    RESERVE_INVENTORY_SCHEMA_VERSION, ReserveInventoryItem, ReserveInventoryPayload,
};
use inventory::consumer::{CONSUMER_NAME, ProcessSummary, SOURCE_PARTITION};
use messaging::{Consumer, Producer, RskafkaConsumer, RskafkaProducer};
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

/// Every test in this binary that reads `inventory.commands.v1` (or its
/// DLQ) shares one real, persistent topic on the dev broker — there is no
/// per-test topic isolation the way `#[sqlx::test]` gives each test its
/// own database. `cargo test` runs tests in parallel by default, so two
/// such tests running at once would otherwise observe each other's
/// records. Any test that publishes to and drains the shared command
/// topic must hold this lock for its full body.
static TOPIC_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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

/// Seeds this test's isolated database's offset ledger to `topic`'s
/// current high watermark, so `process_available` starts from "now"
/// instead of replaying every record ever published to the shared dev
/// topic by earlier test runs.
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

/// Builds a real `inventory.reserve_inventory` envelope (not published),
/// returning its `event_id` and serialized bytes. Kept separate from
/// publishing so a duplicate-delivery test can publish the exact same
/// bytes twice — a real redelivery, not two independently-built events.
pub fn build_reserve_envelope(
    order_id: Uuid,
    order_version: i64,
    items: &[(&str, i64)],
    correlation_id: Uuid,
) -> (Uuid, Vec<u8>) {
    let event_id = Uuid::now_v7();
    let envelope = Envelope {
        event_id,
        event_type: RESERVE_INVENTORY_COMMAND_TYPE.to_string(),
        schema_version: RESERVE_INVENTORY_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: RESERVE_INVENTORY_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        correlation_id,
        causation_id: Uuid::now_v7(),
        traceparent: None,
        payload: ReserveInventoryPayload {
            order_id,
            items: items
                .iter()
                .map(|(sku, qty)| ReserveInventoryItem {
                    sku: sku.to_string(),
                    quantity: *qty,
                })
                .collect(),
            expected_order_version: order_version,
        },
    };
    let bytes = serde_json::to_vec(&envelope).expect("envelope serializes");
    (event_id, bytes)
}

/// Builds and publishes a real `inventory.reserve_inventory` envelope in
/// one step, returning its `event_id`.
pub async fn publish_reserve_command(
    producer: &dyn Producer,
    topic: &str,
    order_id: Uuid,
    order_version: i64,
    items: &[(&str, i64)],
    correlation_id: Uuid,
) -> Uuid {
    let (event_id, bytes) = build_reserve_envelope(order_id, order_version, items, correlation_id);
    publish_raw(producer, topic, &order_id.to_string(), bytes).await;
    event_id
}

/// Publishes a raw, non-envelope payload, for poison-message tests.
pub async fn publish_raw(producer: &dyn Producer, topic: &str, key: &str, bytes: Vec<u8>) {
    producer
        .publish(topic, key, bytes, vec![])
        .await
        .expect("publish raw payload");
}

/// Generous, centralized, and documented per spec section 18: "any
/// unavoidable timing bound is generous, centralized, and documented."
pub const DRAIN_BOUND: Duration = Duration::from_secs(5);

/// Calls `process_available` repeatedly, accumulating summaries, until a
/// fetch comes back with no new records or the fault point stops the
/// batch (whichever first), or [`DRAIN_BOUND`] elapses.
pub async fn drain(
    pool: &PgPool,
    consumer: &dyn Consumer,
    producer: &dyn Producer,
    fault_injector: &FaultInjector,
    topic: &str,
) -> ProcessSummary {
    let mut total = ProcessSummary::default();
    let deadline = tokio::time::Instant::now() + DRAIN_BOUND;
    loop {
        let summary = inventory::consumer::process_available(
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
