use axum::{Router, routing::get};

/// `/health/live`: process is alive. Never checks dependencies.
async fn live() -> &'static str {
    "ok"
}

/// `/health/ready`: checks required dependency connectivity.
///
/// M00 has no database or broker pool wired in yet, so readiness trivially
/// matches liveness. M01 replaces this with a bounded-timeout DB check and
/// M03+ adds a broker check.
async fn ready() -> &'static str {
    "ok"
}

pub fn router() -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_live_returns_200() {
        let app = router();
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

    #[tokio::test]
    async fn health_ready_returns_200() {
        let app = router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
