//! `application/problem+json` error responses (spec section 10), scoped to
//! this service's small dev-only HTTP surface (stock seed/read, fault
//! controls) — the reservation workflow itself is driven by the consumer,
//! not HTTP.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use uuid::Uuid;

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
    #[error("stock not found for sku {0}")]
    StockNotFound(String),
    #[error("missing or invalid X-Test-Token")]
    FaultControlForbidden,
    #[error(transparent)]
    Internal(#[from] sqlx::Error),
}

impl ApiError {
    fn parts(&self, request_id: Uuid) -> (StatusCode, Problem) {
        let (status, code, title) = match self {
            ApiError::StockNotFound(_) => {
                (StatusCode::NOT_FOUND, "STOCK_NOT_FOUND", "Stock Not Found")
            }
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
