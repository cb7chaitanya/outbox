//! Deterministic demonstrations of the naive dual-write stage's two
//! atomicity gaps (spec section 11, M02 acceptance). Requires a real
//! Postgres (via `#[sqlx::test]`) and a real Redpanda broker reachable at
//! `REDPANDA_BROKER` (defaults to the local Compose port) — these are not
//! unit tests, they are the failure lab.

mod common;

use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use contracts::Envelope;
use contracts::orders::OrderCreatedPayload;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

fn broker() -> String {
    std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string())
}

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
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

fn put_fault(name: &str, fail_next: u32) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(format!("/_test/faults/{name}"))
        .header("content-type", "application/json")
        .header("x-test-token", common::TEST_TOKEN)
        .body(Body::from(json!({"fail_next": fail_next}).to_string()))
        .unwrap()
}

fn sample_body() -> Value {
    json!({
        "items": [{"sku": "SKU-DUAL-WRITE", "quantity": 1, "unit_price_minor": 500}],
        "currency": "USD",
    })
}

/// Reads every `orders.order_created` envelope currently on the topic
/// (from `Earliest` to the current high watermark) whose `aggregate_id`
/// matches `order_id`. A bounded, single-shot fetch against a
/// locally-owned dev topic — callers wrap it in their own bounded poll
/// loop rather than this function looping internally, so the timing bound
/// lives in one place (spec section 18).
async fn matching_order_created_records(
    order_id: uuid::Uuid,
) -> Vec<Envelope<OrderCreatedPayload>> {
    let client = ClientBuilder::new(vec![broker()])
        .build()
        .await
        .expect("connect to redpanda");
    let partition = client
        .partition_client(
            contracts::orders::ORDER_CREATED_TOPIC,
            0,
            UnknownTopicHandling::Error,
        )
        .await
        .expect("orders.events.v1 partition client");
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
            let envelope: Envelope<OrderCreatedPayload> = serde_json::from_slice(&value).ok()?;
            (envelope.aggregate_id == order_id).then_some(envelope)
        })
        .collect()
}

/// Generous, centralized, and documented per spec section 18: "any
/// unavoidable timing bound is generous, centralized, and documented."
/// Used both to wait for records that should arrive and, in the negative
/// case, as the window we poll before concluding none ever will.
const POLL_BOUND: Duration = Duration::from_millis(1500);
const POLL_INTERVAL: Duration = Duration::from_millis(150);

#[sqlx::test(migrations = "./migrations")]
async fn dual_write_db_commit_without_event(pool: PgPool) {
    let state = common::live_state(pool.clone()).await;
    let app: Router = orders::http::router(state);

    let configure = app
        .clone()
        .oneshot(put_fault("orders.after_db_commit_before_publish", 1))
        .await
        .unwrap();
    assert_eq!(configure.status(), StatusCode::NO_CONTENT);

    let create_response = app
        .oneshot(post_order("dual-write-gap-1", sample_body()))
        .await
        .unwrap();
    // The DB commit already happened inside repository::create_order; the
    // fault fires right after, before any publish is attempted, so the
    // client sees an injected failure even though the order now exists.
    assert_eq!(create_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let problem = body_json(create_response).await;
    assert_eq!(problem["code"], "INJECTED_FAULT");

    let order_id: uuid::Uuid =
        sqlx::query_scalar("select id from orders where idempotency_key = $1")
            .bind("dual-write-gap-1")
            .fetch_one(&pool)
            .await
            .expect("the order row must exist despite the client-visible failure");

    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + POLL_BOUND;
    while tokio::time::Instant::now() < deadline {
        records = matching_order_created_records(order_id).await;
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert!(
        records.is_empty(),
        "no orders.order_created event should ever have been published for order {order_id}, \
         since the fault fired before the publish attempt; found {records:?}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn dual_write_publish_then_retry_duplicate(pool: PgPool) {
    let state = common::live_state(pool).await;
    let app: Router = orders::http::router(state);

    let configure = app
        .clone()
        .oneshot(put_fault("orders.after_publish_before_response", 1))
        .await
        .unwrap();
    assert_eq!(configure.status(), StatusCode::NO_CONTENT);

    // First attempt: DB commits, publish succeeds, then the fault fires
    // before the response is built. The client sees a failure despite the
    // event already being on the broker.
    let first = app
        .clone()
        .oneshot(post_order("dual-write-gap-2", sample_body()))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::SERVICE_UNAVAILABLE);

    // A naive client retries with the same Idempotency-Key. The DB layer
    // (M01) correctly replays the existing order — no second row — but the
    // naive publish path republishes unconditionally on every accepted
    // call, because nothing couples "was this a replay?" to "should we
    // publish?". That coupling is exactly what the M03 outbox adds.
    let retry = app
        .oneshot(post_order("dual-write-gap-2", sample_body()))
        .await
        .unwrap();
    assert_eq!(retry.status(), StatusCode::ACCEPTED);
    let retry_body = body_json(retry).await;
    let order_id = uuid::Uuid::parse_str(retry_body["id"].as_str().unwrap()).unwrap();

    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + POLL_BOUND;
    while tokio::time::Instant::now() < deadline {
        records = matching_order_created_records(order_id).await;
        if records.len() >= 2 {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert_eq!(
        records.len(),
        2,
        "expected exactly two OrderCreated deliveries for order {order_id} \
         (one from the faulted original request, one from the naive retry's \
         republish); got {records:?}"
    );
    assert_ne!(
        records[0].event_id, records[1].event_id,
        "the two deliveries must be distinct events, not one redelivered record"
    );
    for record in &records {
        assert_eq!(
            record.aggregate_id, order_id,
            "business key (order_id) must match on both deliveries"
        );
    }
}
