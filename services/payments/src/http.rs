use std::sync::Arc;
use std::time::Duration;

use axum::extract::{FromRef, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, put};
use axum::{Json, Router};
use chrono::Utc;
use persistence::outbox::{self, PublishMetrics};
use serde::Deserialize;
use sqlx::PgPool;
use test_support::{FaultConfig, FaultInjector};

use crate::errors::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub fault_injector: Arc<FaultInjector>,
    pub failure_injection_enabled: bool,
    pub failure_injection_token: String,
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
/// timeout (spec section 10).
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
        .route("/metrics", get(metrics));

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
        let pool = PgPool::connect_lazy("postgres://localhost/nonexistent")
            .expect("lazy pool never connects eagerly");
        AppState {
            pool,
            fault_injector: Arc::new(FaultInjector::new()),
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
