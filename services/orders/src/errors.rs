//! `application/problem+json` error responses (spec section 10).

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use uuid::Uuid;

use crate::domain::ValidationError;
use crate::repository::RepoError;

#[derive(Debug, Serialize)]
pub struct Problem {
    #[serde(rename = "type")]
    pub type_: &'static str,
    pub title: &'static str,
    pub status: u16,
    pub code: &'static str,
    pub detail: String,
    pub request_id: Uuid,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("missing or invalid Idempotency-Key header")]
    InvalidIdempotencyKey(String),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("idempotency key reused with a different request body")]
    IdempotencyKeyReused,
    #[error("order not found")]
    OrderNotFound,
    #[error("injected fault fired: {0}")]
    InjectedFault(String),
    #[error("failed to publish event: {0}")]
    PublishFailed(String),
    #[error("missing or invalid X-Test-Token")]
    FaultControlForbidden,
    #[error(transparent)]
    Internal(#[from] sqlx::Error),
}

impl From<RepoError> for ApiError {
    fn from(err: RepoError) -> Self {
        match err {
            RepoError::IdempotencyKeyReused => ApiError::IdempotencyKeyReused,
            RepoError::Sqlx(e) => ApiError::Internal(e),
        }
    }
}

impl ApiError {
    fn parts(&self, request_id: Uuid) -> (StatusCode, Problem) {
        let (status, code, title) = match self {
            ApiError::InvalidIdempotencyKey(_) => (
                StatusCode::BAD_REQUEST,
                "INVALID_IDEMPOTENCY_KEY",
                "Validation Failed",
            ),
            ApiError::Validation(_) => (
                StatusCode::BAD_REQUEST,
                "VALIDATION_FAILED",
                "Validation Failed",
            ),
            ApiError::IdempotencyKeyReused => (
                StatusCode::CONFLICT,
                "IDEMPOTENCY_KEY_REUSED",
                "Idempotency Key Reused",
            ),
            ApiError::OrderNotFound => {
                (StatusCode::NOT_FOUND, "ORDER_NOT_FOUND", "Order Not Found")
            }
            ApiError::InjectedFault(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "INJECTED_FAULT",
                "Injected Fault",
            ),
            ApiError::PublishFailed(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "EVENT_PUBLISH_FAILED",
                "Event Publish Failed",
            ),
            ApiError::FaultControlForbidden => (
                StatusCode::FORBIDDEN,
                "FAULT_CONTROL_FORBIDDEN",
                "Fault Control Forbidden",
            ),
            ApiError::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Internal Error",
            ),
        };
        let problem = Problem {
            type_: "about:blank",
            title,
            status: status.as_u16(),
            code,
            detail: self.to_string(),
            request_id,
        };
        (status, problem)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let ApiError::Internal(ref e) = self {
            tracing::error!(error = %e, "internal error");
        }
        let request_id = Uuid::now_v7();
        let (status, problem) = self.parts(request_id);
        let mut response = (status, Json(problem)).into_response();
        response
            .headers_mut()
            .insert("content-type", "application/problem+json".parse().unwrap());
        response
    }
}
