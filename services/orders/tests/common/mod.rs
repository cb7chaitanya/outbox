//! Shared integration-test scaffolding (spec section 18): builds the
//! `AppState` the router needs, since M02 wired a real producer and fault
//! injector into it.
//!
//! Each `tests/*.rs` file is its own compiled test binary and only uses a
//! subset of these helpers, so per-binary dead-code warnings here are
//! expected, not a real problem.
#![allow(dead_code)]

use std::sync::Arc;

use orders::config::DeliveryMode;
use orders::http::AppState;
use sqlx::PgPool;
use test_support::FaultInjector;

/// Router state for tests that don't care about the Kafka side effect: a
/// `NoopProducer` and an unconfigured fault injector, so `publish_naive`
/// always succeeds silently.
pub fn noop_state(pool: PgPool) -> AppState {
    AppState {
        pool,
        producer: Arc::new(messaging::NoopProducer),
        fault_injector: Arc::new(FaultInjector::new()),
        delivery_mode: DeliveryMode::Naive,
        failure_injection_enabled: false,
        failure_injection_token: String::new(),
    }
}

/// Router state for the dual-write demonstration tests: a real
/// `RskafkaProducer` against the broker named by `REDPANDA_BROKER` (falls
/// back to the local Compose port), with failure injection enabled and a
/// fixed test token so `/_test/faults/*` is reachable.
pub const TEST_TOKEN: &str = "integration-test-token";

pub async fn live_state(pool: PgPool) -> AppState {
    let broker = std::env::var("REDPANDA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
    let producer = messaging::RskafkaProducer::connect(vec![broker])
        .await
        .expect("connect to redpanda for dual-write demonstration test");
    AppState {
        pool,
        producer: Arc::new(producer),
        fault_injector: Arc::new(FaultInjector::new()),
        delivery_mode: DeliveryMode::Naive,
        failure_injection_enabled: true,
        failure_injection_token: TEST_TOKEN.to_string(),
    }
}
