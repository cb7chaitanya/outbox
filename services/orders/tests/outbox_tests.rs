//! Transactional outbox acceptance demonstrations (spec section 20, M03).
//! Requires a real Postgres (via `#[sqlx::test]`) and a real Redpanda
//! broker reachable at `REDPANDA_BROKER` (defaults to the local Compose
//! port).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration as StdDuration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use contracts::orders::{ORDER_AGGREGATE_TYPE, ORDER_CREATED_TOPIC, OrderCreatedPayload};
use messaging::{MessagingError, Producer};
use orders::config::DeliveryMode;
use orders::domain::{ItemRequest, NormalizedOrder};
use orders::repository;
use persistence::outbox::{
    self, FAULT_AFTER_BROKER_PUBLISH_BEFORE_MARK_PUBLISHED, NewOutboxEvent, PublishMetrics,
    PublisherConfig,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use test_support::FaultInjector;
use tower::ServiceExt;
use uuid::Uuid;

fn sample_normalized(currency: &str) -> NormalizedOrder {
    NormalizedOrder {
        items: vec![ItemRequest {
            sku: "SKU-OUTBOX".to_string(),
            quantity: 1,
            unit_price_minor: 500,
        }],
        currency: currency.to_string(),
        amount_minor: 500,
    }
}

fn new_outbox_event(order_id: Uuid, order_version: i64) -> NewOutboxEvent {
    NewOutboxEvent {
        id: Uuid::now_v7(),
        aggregate_type: ORDER_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        topic: ORDER_CREATED_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: json!({
            "event_id": Uuid::now_v7(),
            "event_type": "orders.order_created",
            "schema_version": 1,
            "occurred_at": Utc::now(),
            "producer": "orders",
            "aggregate_type": ORDER_AGGREGATE_TYPE,
            "aggregate_id": order_id,
            "aggregate_version": order_version,
            "correlation_id": Uuid::now_v7(),
            "causation_id": Uuid::now_v7(),
            "payload": {"order_id": order_id, "items": [], "amount": {"currency": "USD", "minor_units": 500}},
        }),
    }
}

fn post_order(key: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/orders")
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn sample_body() -> Value {
    json!({
        "items": [{"sku": "SKU-OUTBOX", "quantity": 1, "unit_price_minor": 500}],
        "currency": "USD",
    })
}

/// Rejects the transaction with a real Postgres constraint violation (a
/// duplicate `order_items` primary key) so the rollback path is exercised
/// against a genuine database error, not a simulated one.
fn duplicate_sku_normalized() -> NormalizedOrder {
    NormalizedOrder {
        items: vec![
            ItemRequest {
                sku: "DUP".to_string(),
                quantity: 1,
                unit_price_minor: 100,
            },
            ItemRequest {
                sku: "DUP".to_string(),
                quantity: 2,
                unit_price_minor: 100,
            },
        ],
        currency: "USD".to_string(),
        amount_minor: 300,
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn transaction_rollback_produces_neither_order_nor_outbox_event(pool: PgPool) {
    let normalized = duplicate_sku_normalized();
    let key = "rollback-test-1";

    let result = repository::create_order(
        &pool,
        key,
        &normalized,
        Uuid::now_v7(),
        Utc::now(),
        |order_id, order_version, _command_version| vec![new_outbox_event(order_id, order_version)],
    )
    .await;

    assert!(
        result.is_err(),
        "the duplicate order_items primary key must fail the transaction"
    );

    let order_count: i64 =
        sqlx::query_scalar("select count(*) from orders where idempotency_key = $1")
            .bind(key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(order_count, 0, "no order row should survive the rollback");

    let outbox_count: i64 = sqlx::query_scalar("select count(*) from outbox_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        outbox_count, 0,
        "no outbox row should survive the rollback (invariant I4)"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn committed_order_has_exactly_one_outbox_row(pool: PgPool) {
    let normalized = sample_normalized("USD");
    let key = "single-outbox-row-1";

    let first = repository::create_order(
        &pool,
        key,
        &normalized,
        Uuid::now_v7(),
        Utc::now(),
        |order_id, order_version, _command_version| vec![new_outbox_event(order_id, order_version)],
    )
    .await
    .unwrap();
    assert!(first.created);

    let count_after_create: i64 =
        sqlx::query_scalar("select count(*) from outbox_events where aggregate_id = $1")
            .bind(first.order.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_after_create, 1,
        "invariant I3: exactly one outbox row per committed business change"
    );

    // An idempotent replay of the same key is not a new business state
    // change, so it must not add a second outbox row.
    let second = repository::create_order(
        &pool,
        key,
        &normalized,
        Uuid::now_v7(),
        Utc::now(),
        |order_id, order_version, _command_version| vec![new_outbox_event(order_id, order_version)],
    )
    .await
    .unwrap();
    assert!(!second.created, "the replay must not create a second order");

    let count_after_replay: i64 =
        sqlx::query_scalar("select count(*) from outbox_events where aggregate_id = $1")
            .bind(first.order.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_after_replay, 1,
        "a replay must not add a second outbox row"
    );
}

/// A [`Producer`] that always succeeds, counting how many times `publish`
/// was called — used to prove a claimed batch is never double-published.
#[derive(Default)]
struct CountingProducer {
    calls: AtomicU64,
}

#[async_trait::async_trait]
impl Producer for CountingProducer {
    async fn publish(
        &self,
        _topic: &str,
        _key: &str,
        _payload: Vec<u8>,
        _headers: Vec<(String, Vec<u8>)>,
    ) -> Result<(), MessagingError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// A [`Producer`] that fails while "down" and succeeds while "up",
/// simulating a broker outage without needing to stop the real Redpanda
/// container (spec section 20 M03 gate 5 permits either approach).
#[derive(Default)]
struct FlakyProducer {
    up: AtomicBool,
}

impl FlakyProducer {
    fn down() -> Self {
        Self {
            up: AtomicBool::new(false),
        }
    }

    fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl Producer for FlakyProducer {
    async fn publish(
        &self,
        _topic: &str,
        _key: &str,
        _payload: Vec<u8>,
        _headers: Vec<(String, Vec<u8>)>,
    ) -> Result<(), MessagingError> {
        if self.up.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(MessagingError::NotImplemented("simulated broker outage"))
        }
    }
}

async fn insert_direct(pool: &PgPool, n: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let order_id = Uuid::now_v7();
        let mut tx = pool.begin().await.unwrap();
        outbox::insert(&mut tx, Utc::now(), &new_outbox_event(order_id, 1))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        ids.push(order_id);
    }
    ids
}

#[sqlx::test(migrations = "./migrations")]
async fn crash_after_publish_before_mark_causes_duplicate_then_eventual_published_mark(
    pool: PgPool,
) {
    let order_id = insert_direct(&pool, 1).await[0];

    let producer = CountingProducer::default();
    let fault_injector = FaultInjector::new();
    fault_injector.configure(
        FAULT_AFTER_BROKER_PUBLISH_BEFORE_MARK_PUBLISHED,
        test_support::FaultConfig {
            fail_next: 1,
            subject_filter: None,
            delay_ms: None,
        },
    );
    let metrics = PublishMetrics::default();
    let config = PublisherConfig {
        claimed_by: "worker-a".to_string(),
        batch_size: 10,
        lease: ChronoDuration::milliseconds(200),
        poll_interval: StdDuration::from_millis(50),
        backoff_base: StdDuration::from_millis(10),
        backoff_cap: StdDuration::from_millis(100),
    };

    // First run: publish succeeds (a real message reaches the broker) but
    // the simulated crash fires before the row is marked published.
    let claimed = outbox::run_publisher_once(
        &pool,
        &producer,
        &fault_injector,
        &config,
        &metrics,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(claimed, 1);
    assert_eq!(producer.calls.load(Ordering::Relaxed), 1);

    let published_at: Option<chrono::DateTime<Utc>> =
        sqlx::query_scalar("select published_at from outbox_events where aggregate_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        published_at.is_none(),
        "the row must stay unpublished after the simulated crash"
    );

    // Wait for the lease to expire, then run the publisher again with no
    // fault configured: it reclaims the row (a lease recovery) and
    // legitimately republishes the identical event — this is the
    // "duplicate delivery" spec section 13 point 6 describes.
    tokio::time::sleep(StdDuration::from_millis(250)).await;
    let claimed_again = outbox::run_publisher_once(
        &pool,
        &producer,
        &fault_injector,
        &config,
        &metrics,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(claimed_again, 1);
    assert_eq!(producer.calls.load(Ordering::Relaxed), 2);

    let (_, _, lease_recoveries, published) = metrics.snapshot();
    assert_eq!(
        lease_recoveries, 1,
        "the second claim must count as a lease recovery"
    );
    assert_eq!(published, 1, "exactly one successful mark-published");

    let published_at: Option<chrono::DateTime<Utc>> =
        sqlx::query_scalar("select published_at from outbox_events where aggregate_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        published_at.is_some(),
        "the row must eventually be marked published"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn two_publishers_share_backlog_without_double_publish(pool: PgPool) {
    const N: usize = 40;
    insert_direct(&pool, N).await;

    let producer = Arc::new(CountingProducer::default());
    let fault_injector = Arc::new(FaultInjector::new());
    let metrics_a = Arc::new(PublishMetrics::default());
    let metrics_b = Arc::new(PublishMetrics::default());

    let config = |claimed_by: &str| PublisherConfig {
        claimed_by: claimed_by.to_string(),
        batch_size: 25,
        lease: ChronoDuration::seconds(30),
        poll_interval: StdDuration::from_millis(50),
        backoff_base: StdDuration::from_millis(10),
        backoff_cap: StdDuration::from_millis(100),
    };

    let (pool_a, producer_a, fault_a, metrics_a2) = (
        pool.clone(),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        Arc::clone(&metrics_a),
    );
    let config_a = config("worker-a");
    let handle_a = tokio::spawn(async move {
        outbox::run_publisher_once(
            &pool_a,
            producer_a.as_ref(),
            &fault_a,
            &config_a,
            &metrics_a2,
            Utc::now(),
        )
        .await
        .unwrap()
    });

    let (pool_b, producer_b, fault_b, metrics_b2) = (
        pool.clone(),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        Arc::clone(&metrics_b),
    );
    let config_b = config("worker-b");
    let handle_b = tokio::spawn(async move {
        outbox::run_publisher_once(
            &pool_b,
            producer_b.as_ref(),
            &fault_b,
            &config_b,
            &metrics_b2,
            Utc::now(),
        )
        .await
        .unwrap()
    });

    let (claimed_a, claimed_b) = tokio::join!(handle_a, handle_b);
    let claimed_a = claimed_a.unwrap();
    let claimed_b = claimed_b.unwrap();

    assert_eq!(
        claimed_a + claimed_b,
        N,
        "the two concurrent claims must partition the backlog exactly, with no overlap and no gaps"
    );
    assert_eq!(
        producer.calls.load(Ordering::Relaxed) as usize,
        N,
        "every row must be published exactly once total, never twice"
    );

    let unpublished: i64 =
        sqlx::query_scalar("select count(*) from outbox_events where published_at is null")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        unpublished, 0,
        "both workers together must drain the whole backlog"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn broker_outage_grows_backlog_then_drains_on_recovery(pool: PgPool) {
    const N: usize = 5;
    insert_direct(&pool, N).await;

    let producer = FlakyProducer::down();
    let fault_injector = FaultInjector::new();
    let metrics = PublishMetrics::default();
    let config = PublisherConfig {
        claimed_by: "outage-worker".to_string(),
        batch_size: 10,
        lease: ChronoDuration::seconds(30),
        poll_interval: StdDuration::from_millis(20),
        backoff_base: StdDuration::from_millis(10),
        backoff_cap: StdDuration::from_millis(30),
    };

    // While the broker is "down", every attempt fails: the backlog does
    // not shrink.
    outbox::run_publisher_once(
        &pool,
        &producer,
        &fault_injector,
        &config,
        &metrics,
        Utc::now(),
    )
    .await
    .unwrap();
    let backlog_during_outage = outbox::backlog_metrics(&pool, Utc::now()).await.unwrap();
    assert_eq!(
        backlog_during_outage.unpublished_count, N as i64,
        "the backlog must not shrink while the broker is unreachable"
    );
    let (_, failures, _, _) = metrics.snapshot();
    assert!(
        failures >= N as u64,
        "every attempt during the outage must be recorded as a failure"
    );

    // Recovery: wait past the backoff cap, flip the producer "up", and
    // keep polling until the backlog drains.
    tokio::time::sleep(StdDuration::from_millis(50)).await;
    producer.set_up(true);

    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(3);
    let mut backlog = N as i64;
    while tokio::time::Instant::now() < deadline && backlog > 0 {
        outbox::run_publisher_once(
            &pool,
            &producer,
            &fault_injector,
            &config,
            &metrics,
            Utc::now(),
        )
        .await
        .unwrap();
        backlog = outbox::backlog_metrics(&pool, Utc::now())
            .await
            .unwrap()
            .unpublished_count;
        if backlog > 0 {
            tokio::time::sleep(StdDuration::from_millis(15)).await;
        }
    }

    assert_eq!(
        backlog, 0,
        "the backlog must fully drain once the broker recovers"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn outbox_mode_closes_the_naive_lost_event_window(pool: PgPool) {
    let state = common::live_state_with_mode(pool.clone(), DeliveryMode::Outbox).await;
    let app: Router = orders::http::router(state.clone());

    // Even with the naive fault point configured, outbox mode never calls
    // the naive publish path at all, so this has no effect on the outcome
    // — the point being demonstrated is structural, not merely untriggered.
    let configure = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/_test/faults/orders.after_db_commit_before_publish")
                .header("content-type", "application/json")
                .header("x-test-token", common::TEST_TOKEN)
                .body(Body::from(json!({"fail_next": 1}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(configure.status(), StatusCode::NO_CONTENT);

    let create_response = app
        .oneshot(post_order("outbox-lost-event-1", sample_body()))
        .await
        .unwrap();
    assert_eq!(
        create_response.status(),
        StatusCode::ACCEPTED,
        "outbox mode must not fail the request on a fault point it never reaches"
    );
    let body = common::body_json(create_response).await;
    let order_id = Uuid::parse_str(body["id"].as_str().unwrap()).unwrap();

    // The event is durably recorded atomically with the order — no
    // publisher run required to prove it exists.
    let outbox_row_exists: bool =
        sqlx::query_scalar("select exists(select 1 from outbox_events where aggregate_id = $1)")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        outbox_row_exists,
        "the outbox row must exist immediately, before any publisher has ever run"
    );

    // Running the publisher then delivers it for real.
    let metrics = PublishMetrics::default();
    outbox::run_publisher_once(
        &pool,
        state.producer.as_ref(),
        &FaultInjector::new(),
        &PublisherConfig::default(),
        &metrics,
        Utc::now(),
    )
    .await
    .unwrap();

    let records = common::poll_until::<OrderCreatedPayload>(ORDER_CREATED_TOPIC, order_id, 1).await;
    assert_eq!(
        records.len(),
        1,
        "the outbox-published event must eventually reach the broker exactly once"
    );
}
