//! `application/problem+json` error responses (spec section 10), scoped to
//! this service's small HTTP surface (health, metrics, fault controls) —
//! the authorize/refund workflow itself is driven by the consumer, not
//! HTTP, matching inventory's M04 shape.

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
    #[error("missing or invalid X-Test-Token")]
    FaultControlForbidden,
}

impl ApiError {
    fn parts(&self, request_id: Uuid) -> (StatusCode, Problem) {
        let (status, code, title) = match self {
            ApiError::FaultControlForbidden => (
                StatusCode::FORBIDDEN,
                "FAULT_CONTROL_FORBIDDEN",
                "Fault Control Forbidden",
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
        let request_id = Uuid::now_v7();
        let (status, problem) = self.parts(request_id);
        let mut response = (status, Json(problem)).into_response();
        response
            .headers_mut()
            .insert("content-type", "application/problem+json".parse().unwrap());
        response
    }
}
