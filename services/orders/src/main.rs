use std::sync::Arc;

use messaging::RskafkaProducer;
use orders::config::{Config, DatabaseConfig, FailureInjectionConfig, MessagingConfig};
use orders::http::{self, AppState};
use sqlx::postgres::PgPoolOptions;
use test_support::FaultInjector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing("orders");

    let config = Config::load()?;
    let db_config = DatabaseConfig::load()?;
    let messaging_config = MessagingConfig::load()?;
    let failure_injection_config = FailureInjectionConfig::load()?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&db_config.database_url())
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let producer = RskafkaProducer::connect(vec![messaging_config.redpanda_broker.clone()]).await?;

    if failure_injection_config.enabled() {
        tracing::warn!("FAILURE_INJECTION_ENABLED=true: /_test/faults/* is mounted");
    }

    let state = AppState {
        pool,
        producer: Arc::new(producer),
        fault_injector: Arc::new(FaultInjector::new()),
        delivery_mode: config.delivery_mode,
        failure_injection_enabled: failure_injection_config.enabled(),
        failure_injection_token: failure_injection_config.failure_injection_token.clone(),
    };

    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(addr = %config.socket_addr(), "orders listening");

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install ctrl-c handler");
}
