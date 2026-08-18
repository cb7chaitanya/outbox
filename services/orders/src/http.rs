use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::{CreateOrderRequest, OrderStatus, validate_and_normalize};
use crate::errors::ApiError;
use crate::repository::{self, OrderItemRow, OrderRow, TransitionRow};

const MIN_IDEMPOTENCY_KEY_LEN: usize = 8;
const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;

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

async fn create_order(
    State(pool): State<PgPool>,
    headers: HeaderMap,
    Json(body): Json<CreateOrderRequest>,
) -> Result<Response, ApiError> {
    let idempotency_key = extract_idempotency_key(&headers)?;
    let normalized = validate_and_normalize(body)?;

    let outcome =
        repository::create_order(&pool, &idempotency_key, &normalized, Utc::now()).await?;

    let representation = OrderRepresentation::from_row(outcome.order, outcome.items);
    let location = representation.links.self_.clone();
    let mut response = (StatusCode::ACCEPTED, Json(representation)).into_response();
    response.headers_mut().insert(
        header::LOCATION,
        location.parse().expect("order id is a valid header value"),
    );
    Ok(response)
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

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/orders", post(create_order))
        .route("/v1/orders/{order_id}", get(get_order))
        .route("/v1/orders/{order_id}/transitions", get(get_transitions))
        .with_state(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_pool() -> PgPool {
        // Health-endpoint tests don't touch the database; a lazily-connecting
        // pool is enough since `live` never queries it and these tests never
        // call `ready`.
        PgPool::connect_lazy("postgres://localhost/nonexistent")
            .expect("lazy pool never connects eagerly")
    }

    #[tokio::test]
    async fn health_live_returns_200() {
        let app = router(test_pool());
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
