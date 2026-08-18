//! HTTP-layer tests against a real Postgres database (spec section 18,
//! section 20 M01 acceptance gates).

mod common;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

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

fn sample_body() -> Value {
    json!({
        "items": [{"sku": "SKU-1", "quantity": 2, "unit_price_minor": 1250}],
        "currency": "USD",
    })
}

#[sqlx::test(migrations = "./migrations")]
async fn correlation_id_is_echoed_when_supplied_and_generated_when_absent(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));

    let supplied = uuid::Uuid::now_v7().to_string();
    let mut with_header = post_order("corr-key-001", sample_body());
    with_header
        .headers_mut()
        .insert("x-correlation-id", supplied.parse().unwrap());
    let response = app.clone().oneshot(with_header).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let echoed = response
        .headers()
        .get("x-correlation-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(echoed, supplied);
    let body = body_json(response).await;
    assert_eq!(body["correlation_id"], supplied);

    let without_header = post_order("corr-key-002", sample_body());
    let response = app.oneshot(without_header).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let generated = response
        .headers()
        .get("x-correlation-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        uuid::Uuid::parse_str(&generated).is_ok(),
        "a UUID must be generated when absent"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn create_then_get_order_and_transitions_round_trip(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));

    let create_response = app
        .clone()
        .oneshot(post_order("http-key-001", sample_body()))
        .await
        .unwrap();
    assert_eq!(create_response.status(), StatusCode::ACCEPTED);
    let location = create_response
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let created = body_json(create_response).await;
    assert_eq!(created["status"], "PENDING");
    assert_eq!(created["amount_minor"], 2500);
    let order_id = created["id"].as_str().unwrap();
    assert!(location.ends_with(order_id));

    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(location.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
    let fetched = body_json(get_response).await;
    assert_eq!(fetched["id"], created["id"]);
    assert_eq!(fetched["version"], 1);

    let transitions_response = app
        .oneshot(
            Request::builder()
                .uri(format!("{location}/transitions"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(transitions_response.status(), StatusCode::OK);
    let transitions = body_json(transitions_response).await;
    assert_eq!(transitions.as_array().unwrap().len(), 1);
    assert_eq!(transitions[0]["to_status"], "PENDING");
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_identical_http_requests_yield_one_order_and_same_response(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let response = app
                .oneshot(post_order("http-concurrent-001", sample_body()))
                .await
                .unwrap();
            let status = response.status();
            (status, body_json(response).await)
        }));
    }

    let mut ids = std::collections::HashSet::new();
    let mut bodies = std::collections::HashSet::new();
    for handle in handles {
        let (status, body) = handle.await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        ids.insert(body["id"].as_str().unwrap().to_string());
        // created_at/updated_at are identical across replays since the row
        // is never re-written, so the full body is comparable byte-for-byte
        // once serialized.
        bodies.insert(body.to_string());
    }

    assert_eq!(
        ids.len(),
        1,
        "all concurrent HTTP callers must observe the same order id"
    );
    assert_eq!(
        bodies.len(),
        1,
        "all concurrent HTTP callers must observe the same response body"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn reused_idempotency_key_different_body_returns_409(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));

    let first = app
        .clone()
        .oneshot(post_order("http-key-002", sample_body()))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);

    let different = json!({
        "items": [{"sku": "SKU-9", "quantity": 1, "unit_price_minor": 1}],
        "currency": "USD",
    });
    let second = app
        .oneshot(post_order("http-key-002", different))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::CONFLICT);
    let problem = body_json(second).await;
    assert_eq!(problem["code"], "IDEMPOTENCY_KEY_REUSED");
}

#[sqlx::test(migrations = "./migrations")]
async fn missing_idempotency_key_returns_400(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));
    let request = Request::builder()
        .method("POST")
        .uri("/v1/orders")
        .header("content-type", "application/json")
        .body(Body::from(sample_body().to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = body_json(response).await;
    assert_eq!(problem["code"], "INVALID_IDEMPOTENCY_KEY");
}

#[sqlx::test(migrations = "./migrations")]
async fn invalid_items_return_400_validation_failed(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));
    let empty_items = json!({"items": [], "currency": "USD"});
    let response = app
        .oneshot(post_order("http-key-003", empty_items))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = body_json(response).await;
    assert_eq!(problem["code"], "VALIDATION_FAILED");
}

#[sqlx::test(migrations = "./migrations")]
async fn get_missing_order_returns_404(pool: PgPool) {
    let app: Router = orders::http::router(common::noop_state(pool));
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/orders/{}", uuid::Uuid::now_v7()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let problem = body_json(response).await;
    assert_eq!(problem["code"], "ORDER_NOT_FOUND");
}
