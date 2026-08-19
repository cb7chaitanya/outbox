use std::sync::Arc;
use std::time::Duration;

use axum::extract::{FromRef, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use contracts::Envelope;
use contracts::inventory::{
    INVENTORY_COMMANDS_TOPIC, RESERVE_INVENTORY_AGGREGATE_TYPE, RESERVE_INVENTORY_COMMAND_TYPE,
    RESERVE_INVENTORY_SCHEMA_VERSION, ReserveInventoryItem, ReserveInventoryPayload,
};
use contracts::orders::{
    ORDER_AGGREGATE_TYPE, ORDER_CREATED_EVENT_TYPE, ORDER_CREATED_SCHEMA_VERSION,
    ORDER_CREATED_TOPIC, ORDERS_PRODUCER_NAME, OrderCreatedAmount, OrderCreatedItem,
    OrderCreatedPayload,
};
use messaging::Producer;
use persistence::outbox::{self, NewOutboxEvent, PublishMetrics};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use test_support::{FaultConfig, FaultInjector};
use uuid::Uuid;

use crate::config::DeliveryMode;
use crate::domain::{CreateOrderRequest, OrderStatus, validate_and_normalize};
use crate::errors::ApiError;
use crate::repository::{self, CreateOutcome, OrderItemRow, OrderRow, TransitionRow};

const MIN_IDEMPOTENCY_KEY_LEN: usize = 8;
const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;

/// Fault point names (spec section 11): the naive dual-write flow's two
/// atomicity gaps.
pub const FAULT_AFTER_DB_COMMIT_BEFORE_PUBLISH: &str = "orders.after_db_commit_before_publish";
pub const FAULT_AFTER_PUBLISH_BEFORE_RESPONSE: &str = "orders.after_publish_before_response";

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub producer: Arc<dyn Producer>,
    pub fault_injector: Arc<FaultInjector>,
    pub delivery_mode: DeliveryMode,
    pub failure_injection_enabled: bool,
    pub failure_injection_token: String,
    /// Shared with the background outbox publisher task (spec section 16);
    /// backlog count and oldest-unpublished age are queried live from the
    /// database instead, since they describe the table, not the worker.
    pub publish_metrics: Arc<PublishMetrics>,
}

impl FromRef<AppState> for PgPool {
    fn from_ref(state: &AppState) -> Self {
        state.pool.clone()
    }
}

/// `/health/live`: process is alive. Never checks dependencies.
async fn live() -> &'static str {
    "ok"
}

/// `/health/ready`: checks required dependency connectivity with a bounded
/// timeout (spec section 10). M03+ adds a broker check alongside this.
async fn ready(State(pool): State<PgPool>) -> impl IntoResponse {
    match tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query("select 1").execute(&pool),
    )
    .await
    {
        Ok(Ok(_)) => (StatusCode::OK, "ok").into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response(),
    }
}

/// `GET /metrics`: Prometheus text format (spec section 10, section 16).
/// M03 exposes only the outbox counters that milestone actually produces;
/// HTTP latency, consumer lag, and the rest of the full catalog land in
/// M09 alongside OpenTelemetry wiring.
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let backlog = outbox::backlog_metrics(&state.pool, Utc::now())
        .await
        .unwrap_or_default();
    let (attempts, failures, lease_recoveries, published) = state.publish_metrics.snapshot();

    let mut body = String::new();
    body.push_str("# HELP outbox_unpublished_count Unpublished outbox rows.\n");
    body.push_str("# TYPE outbox_unpublished_count gauge\n");
    body.push_str(&format!(
        "outbox_unpublished_count {}\n",
        backlog.unpublished_count
    ));
    body.push_str(
        "# HELP outbox_oldest_unpublished_age_seconds Age of the oldest unpublished row.\n",
    );
    body.push_str("# TYPE outbox_oldest_unpublished_age_seconds gauge\n");
    body.push_str(&format!(
        "outbox_oldest_unpublished_age_seconds {}\n",
        backlog.oldest_unpublished_age_seconds.unwrap_or(0.0)
    ));
    body.push_str("# HELP outbox_publish_attempts_total Publish attempts made by this worker.\n");
    body.push_str("# TYPE outbox_publish_attempts_total counter\n");
    body.push_str(&format!("outbox_publish_attempts_total {attempts}\n"));
    body.push_str("# HELP outbox_publish_failures_total Publish attempts that failed.\n");
    body.push_str("# TYPE outbox_publish_failures_total counter\n");
    body.push_str(&format!("outbox_publish_failures_total {failures}\n"));
    body.push_str("# HELP outbox_publish_success_total Rows successfully marked published.\n");
    body.push_str("# TYPE outbox_publish_success_total counter\n");
    body.push_str(&format!("outbox_publish_success_total {published}\n"));
    body.push_str(
        "# HELP outbox_lease_recoveries_total Rows reclaimed after a prior lease expired.\n",
    );
    body.push_str("# TYPE outbox_lease_recoveries_total counter\n");
    body.push_str(&format!(
        "outbox_lease_recoveries_total {lease_recoveries}\n"
    ));

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

#[derive(Debug, Serialize)]
struct ItemRepresentation {
    sku: String,
    quantity: i64,
    unit_price_minor: i64,
}

#[derive(Debug, Serialize)]
struct OrderRepresentation {
    id: Uuid,
    status: OrderStatus,
    currency: String,
    amount_minor: i64,
    version: i64,
    cancellation_reason: Option<String>,
    correlation_id: Uuid,
    items: Vec<ItemRepresentation>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    links: Links,
}

#[derive(Debug, Serialize)]
struct Links {
    #[serde(rename = "self")]
    self_: String,
    transitions: String,
}

impl OrderRepresentation {
    fn from_row(order: OrderRow, items: Vec<OrderItemRow>) -> Self {
        let links = Links {
            self_: format!("/v1/orders/{}", order.id),
            transitions: format!("/v1/orders/{}/transitions", order.id),
        };
        Self {
            id: order.id,
            status: order.status,
            currency: order.currency,
            amount_minor: order.amount_minor,
            version: order.version,
            cancellation_reason: order.cancellation_reason,
            correlation_id: order.correlation_id,
            items: items
                .into_iter()
                .map(|item| ItemRepresentation {
                    sku: item.sku,
                    quantity: item.quantity,
                    unit_price_minor: item.unit_price_minor,
                })
                .collect(),
            created_at: order.created_at,
            updated_at: order.updated_at,
            links,
        }
    }
}

#[derive(Debug, Serialize)]
struct TransitionRepresentation {
    id: Uuid,
    from_status: Option<OrderStatus>,
    to_status: OrderStatus,
    reason: Option<String>,
    order_version: i64,
    created_at: DateTime<Utc>,
}

impl From<TransitionRow> for TransitionRepresentation {
    fn from(row: TransitionRow) -> Self {
        Self {
            id: row.id,
            from_status: row.from_status,
            to_status: row.to_status,
            reason: row.reason,
            order_version: row.order_version,
            created_at: row.created_at,
        }
    }
}

fn extract_idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let raw = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::InvalidIdempotencyKey("Idempotency-Key header is required".to_string())
        })?;

    let len = raw.chars().count();
    let printable = raw.chars().all(|c| c.is_ascii_graphic() || c == ' ');
    if !(MIN_IDEMPOTENCY_KEY_LEN..=MAX_IDEMPOTENCY_KEY_LEN).contains(&len) || !printable {
        return Err(ApiError::InvalidIdempotencyKey(format!(
            "Idempotency-Key must be {MIN_IDEMPOTENCY_KEY_LEN}-{MAX_IDEMPOTENCY_KEY_LEN} printable characters"
        )));
    }
    Ok(raw.to_string())
}

/// Uses the caller-supplied `X-Correlation-ID` if it parses as a UUID,
/// otherwise generates one (spec section 10: "optional UUID; generate one
/// if absent"; an unparseable value is treated the same as absent rather
/// than rejected, since correlation IDs are a tracing aid, not a
/// correctness input).
fn extract_or_generate_correlation_id(headers: &HeaderMap) -> Uuid {
    headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok())
        .unwrap_or_else(Uuid::now_v7)
}

async fn create_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateOrderRequest>,
) -> Result<Response, ApiError> {
    let idempotency_key = extract_idempotency_key(&headers)?;
    let correlation_id = extract_or_generate_correlation_id(&headers);
    let normalized = validate_and_normalize(body)?;

    // Built once, up front, so each outbox row's event id stays stable even
    // if a caller-level retry re-invokes this handler (spec section 13).
    // Only used when `delivery_mode == Outbox`; the closure checks that
    // itself so `create_order` doesn't need to know about delivery modes.
    let order_created_event_id = Uuid::now_v7();
    let reserve_inventory_event_id = Uuid::now_v7();
    let causation_id = Uuid::now_v7();
    let delivery_mode = state.delivery_mode;
    let event_items: Vec<OrderCreatedItem> = normalized
        .items
        .iter()
        .map(|item| OrderCreatedItem {
            sku: item.sku.clone(),
            quantity: item.quantity,
        })
        .collect();
    let event_currency = normalized.currency.clone();
    let event_amount_minor = normalized.amount_minor;

    let outcome = repository::create_order(
        &state.pool,
        &idempotency_key,
        &normalized,
        correlation_id,
        Utc::now(),
        move |order_id, order_version| {
            if delivery_mode != DeliveryMode::Outbox {
                return Vec::new();
            }
            let order_created_envelope = build_order_created_envelope(
                order_created_event_id,
                order_id,
                order_version,
                event_items.clone(),
                event_currency,
                event_amount_minor,
                correlation_id,
                causation_id,
            );
            let order_created_json =
                serde_json::to_value(&order_created_envelope).expect("envelope serializes to json");

            let reserve_inventory_envelope = Envelope {
                event_id: reserve_inventory_event_id,
                event_type: RESERVE_INVENTORY_COMMAND_TYPE.to_string(),
                schema_version: RESERVE_INVENTORY_SCHEMA_VERSION,
                occurred_at: Utc::now(),
                producer: ORDERS_PRODUCER_NAME.to_string(),
                aggregate_type: RESERVE_INVENTORY_AGGREGATE_TYPE.to_string(),
                aggregate_id: order_id,
                aggregate_version: order_version,
                correlation_id,
                causation_id,
                traceparent: None,
                payload: ReserveInventoryPayload {
                    order_id,
                    items: event_items
                        .into_iter()
                        .map(|item| ReserveInventoryItem {
                            sku: item.sku,
                            quantity: item.quantity,
                        })
                        .collect(),
                    expected_order_version: order_version,
                },
            };
            let reserve_inventory_json = serde_json::to_value(&reserve_inventory_envelope)
                .expect("envelope serializes to json");

            vec![
                NewOutboxEvent {
                    id: order_created_event_id,
                    aggregate_type: ORDER_AGGREGATE_TYPE.to_string(),
                    aggregate_id: order_id,
                    aggregate_version: order_version,
                    topic: ORDER_CREATED_TOPIC.to_string(),
                    message_key: order_id.to_string(),
                    envelope: order_created_json,
                },
                NewOutboxEvent {
                    id: reserve_inventory_event_id,
                    aggregate_type: RESERVE_INVENTORY_AGGREGATE_TYPE.to_string(),
                    aggregate_id: order_id,
                    aggregate_version: order_version,
                    topic: INVENTORY_COMMANDS_TOPIC.to_string(),
                    message_key: order_id.to_string(),
                    envelope: reserve_inventory_json,
                },
            ]
        },
    )
    .await?;

    if state.delivery_mode == DeliveryMode::Naive {
        publish_naive(&state, &outcome, correlation_id).await?;
    }

    let representation = OrderRepresentation::from_row(outcome.order, outcome.items);
    let location = representation.links.self_.clone();
    let response_correlation_id = representation.correlation_id;
    let mut response = (StatusCode::ACCEPTED, Json(representation)).into_response();
    let headers_mut = response.headers_mut();
    headers_mut.insert(
        header::LOCATION,
        location.parse().expect("order id is a valid header value"),
    );
    headers_mut.insert(
        "x-correlation-id",
        response_correlation_id
            .to_string()
            .parse()
            .expect("uuid string is a valid header value"),
    );
    Ok(response)
}

/// Kafka message headers carrying `event_id` and correlation/causation IDs
/// (spec section 13 point 3), independent of the delivery mode.
fn event_headers(
    event_id: &Uuid,
    correlation_id: Uuid,
    causation_id: Uuid,
) -> Vec<(String, Vec<u8>)> {
    vec![
        ("event_id".to_string(), event_id.to_string().into_bytes()),
        (
            "correlation_id".to_string(),
            correlation_id.to_string().into_bytes(),
        ),
        (
            "causation_id".to_string(),
            causation_id.to_string().into_bytes(),
        ),
        (
            "producer".to_string(),
            ORDERS_PRODUCER_NAME.to_string().into_bytes(),
        ),
    ]
}

/// Builds an `orders.order_created` envelope. Shared by the naive publish
/// path (which has an already-committed `CreateOutcome` to read fields
/// from) and the outbox event builder (which only has the order id and
/// version the repository just assigned, plus the pre-validated request).
#[allow(clippy::too_many_arguments)]
fn build_order_created_envelope(
    event_id: Uuid,
    order_id: Uuid,
    order_version: i64,
    items: Vec<OrderCreatedItem>,
    currency: String,
    amount_minor: i64,
    correlation_id: Uuid,
    causation_id: Uuid,
) -> Envelope<OrderCreatedPayload> {
    Envelope {
        event_id,
        event_type: ORDER_CREATED_EVENT_TYPE.to_string(),
        schema_version: ORDER_CREATED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: ORDERS_PRODUCER_NAME.to_string(),
        aggregate_type: ORDER_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        correlation_id,
        causation_id,
        traceparent: None,
        payload: OrderCreatedPayload {
            order_id,
            items,
            amount: OrderCreatedAmount {
                currency,
                minor_units: amount_minor,
            },
        },
    }
}

/// The naive dual-write path (spec section 11): after the order row is
/// committed, publish `orders.order_created` directly to Kafka and return
/// `202` only if both steps succeed. Deliberately republishes on every
/// accepted call, including idempotent replays (`outcome.created == false`)
/// — coupling the publish-skip decision to the idempotency layer would
/// already be the start of an outbox-shaped fix, and the whole point of
/// this milestone is to show that coupling is missing here. See
/// `docs/adr/0003-naive-publish-on-every-replay.md`.
async fn publish_naive(
    state: &AppState,
    outcome: &CreateOutcome,
    correlation_id: Uuid,
) -> Result<(), ApiError> {
    let order_id = outcome.order.id;
    let subject = order_id.to_string();

    if outcome.created {
        state
            .fault_injector
            .maybe_fail(FAULT_AFTER_DB_COMMIT_BEFORE_PUBLISH, Some(&subject))
            .await
            .map_err(|f| ApiError::InjectedFault(f.name))?;
    }

    let items = outcome
        .items
        .iter()
        .map(|item| OrderCreatedItem {
            sku: item.sku.clone(),
            quantity: item.quantity,
        })
        .collect();
    let envelope = build_order_created_envelope(
        Uuid::now_v7(),
        order_id,
        outcome.order.version,
        items,
        outcome.order.currency.clone(),
        outcome.order.amount_minor,
        correlation_id,
        Uuid::now_v7(),
    );
    let bytes = serde_json::to_vec(&envelope).expect("envelope serialization cannot fail");
    let headers = event_headers(
        &envelope.event_id,
        envelope.correlation_id,
        envelope.causation_id,
    );

    state
        .producer
        .publish(ORDER_CREATED_TOPIC, &subject, bytes, headers)
        .await
        .map_err(|e| ApiError::PublishFailed(e.to_string()))?;

    state
        .fault_injector
        .maybe_fail(FAULT_AFTER_PUBLISH_BEFORE_RESPONSE, Some(&subject))
        .await
        .map_err(|f| ApiError::InjectedFault(f.name))?;

    Ok(())
}

async fn get_order(
    State(pool): State<PgPool>,
    Path(order_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let (order, items) = repository::get_order(&pool, order_id)
        .await?
        .ok_or(ApiError::OrderNotFound)?;
    Ok(Json(OrderRepresentation::from_row(order, items)).into_response())
}

async fn get_transitions(
    State(pool): State<PgPool>,
    Path(order_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let transitions = repository::get_transitions(&pool, order_id)
        .await?
        .ok_or(ApiError::OrderNotFound)?;
    let representation: Vec<TransitionRepresentation> =
        transitions.into_iter().map(Into::into).collect();
    Ok(Json(representation).into_response())
}

#[derive(Debug, Deserialize)]
struct FaultConfigRequest {
    fail_next: u32,
    #[serde(default)]
    subject_filter: Option<String>,
    #[serde(default)]
    delay_ms: Option<u64>,
}

fn check_test_token(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let supplied = headers
        .get("x-test-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if supplied.is_empty() || supplied != state.failure_injection_token {
        return Err(ApiError::FaultControlForbidden);
    }
    Ok(())
}

/// `PUT /_test/faults/{name}` (spec section 17): dev/test-only, requires a
/// matching `X-Test-Token`, only mounted when failure injection is enabled.
async fn put_fault(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<FaultConfigRequest>,
) -> Result<StatusCode, ApiError> {
    check_test_token(&state, &headers)?;
    let config = FaultConfig {
        fail_next: body.fail_next,
        subject_filter: body.subject_filter,
        delay_ms: body.delay_ms,
    };
    tracing::info!(fault = %name, ?config, "fault injector configured");
    state.fault_injector.configure(&name, config);
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /_test/faults`: clears every configured fault.
async fn delete_faults(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    check_test_token(&state, &headers)?;
    tracing::info!("fault injector cleared");
    state.fault_injector.clear();
    Ok(StatusCode::NO_CONTENT)
}

pub fn router(state: AppState) -> Router {
    let router = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/orders", post(create_order))
        .route("/v1/orders/{order_id}", get(get_order))
        .route("/v1/orders/{order_id}/transitions", get(get_transitions));

    let router = if state.failure_injection_enabled {
        router
            .route("/_test/faults/{name}", put(put_fault))
            .route("/_test/faults", delete(delete_faults))
    } else {
        router
    };

    router.with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        // Health-endpoint tests don't touch the database; a lazily-connecting
        // pool is enough since `live` never queries it and these tests never
        // call `ready`.
        let pool = PgPool::connect_lazy("postgres://localhost/nonexistent")
            .expect("lazy pool never connects eagerly");
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

    #[tokio::test]
    async fn health_live_returns_200() {
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
