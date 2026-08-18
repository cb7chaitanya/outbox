//! Tracing setup, metrics, and W3C trace-context propagation shared by
//! every service. M00 wires structured JSON logging only; OpenTelemetry
//! spans and Prometheus metrics land in M09.

use tracing_subscriber::EnvFilter;

/// Installs a JSON-formatted tracing subscriber, honoring `RUST_LOG`.
pub fn init_tracing(service_name: &'static str) {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_current_span(false)
        .init();
    tracing::info!(service = service_name, "tracing initialized");
}
