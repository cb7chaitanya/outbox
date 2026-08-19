use std::sync::Arc;

use messaging::{RskafkaConsumer, RskafkaProducer};
use payments::config::{Config, DatabaseConfig, FailureInjectionConfig, MessagingConfig};
use payments::http::{self, AppState};
use payments::provider::{FakeProvider, PaymentProvider};
use persistence::outbox::PublishMetrics;
use sqlx::postgres::PgPoolOptions;
use test_support::FaultInjector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing("payments");

    let config = Config::load()?;
    let db_config = DatabaseConfig::load()?;
    let messaging_config = MessagingConfig::load()?;
    let failure_injection_config = FailureInjectionConfig::load()?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&db_config.database_url())
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let producer: Arc<dyn messaging::Producer> =
        Arc::new(RskafkaProducer::connect(vec![messaging_config.redpanda_broker.clone()]).await?);
    let consumer: Arc<dyn messaging::Consumer> =
        Arc::new(RskafkaConsumer::connect(vec![messaging_config.redpanda_broker.clone()]).await?);

    if failure_injection_config.enabled() {
        tracing::warn!("FAILURE_INJECTION_ENABLED=true: /_test/faults/* is mounted");
    }

    let fault_injector = Arc::new(FaultInjector::new());
    let provider: Arc<dyn PaymentProvider> =
        Arc::new(FakeProvider::new(Arc::clone(&fault_injector)));
    let publish_metrics = Arc::new(PublishMetrics::default());

    let publisher_claimed_by = format!("payments-{}", uuid::Uuid::now_v7());
    let publisher_handle = persistence::outbox::spawn_publisher_loop(
        pool.clone(),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        config.publisher_config(publisher_claimed_by),
        Arc::clone(&publish_metrics),
    );

    let consumer_handle = spawn_consumer_loop(
        pool.clone(),
        Arc::clone(&consumer),
        Arc::clone(&producer),
        Arc::clone(&provider),
        Arc::clone(&fault_injector),
        config.consumer_max_wait_ms,
        config.consumer_poll_interval_ms,
        config.provider_retry_config(),
    );

    let state = AppState {
        pool,
        fault_injector,
        failure_injection_enabled: failure_injection_config.enabled(),
        failure_injection_token: failure_injection_config.failure_injection_token.clone(),
        publish_metrics,
    };

    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(addr = %config.socket_addr(), "payments listening");

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    publisher_handle.abort();
    consumer_handle.abort();

    Ok(())
}

/// Polls `payments.commands.v1` forever, running the idempotent-inbox
/// protocol (`payments::consumer::process_available`) on whatever's new
/// each tick.
#[allow(clippy::too_many_arguments)]
fn spawn_consumer_loop(
    pool: sqlx::PgPool,
    consumer: Arc<dyn messaging::Consumer>,
    producer: Arc<dyn messaging::Producer>,
    provider: Arc<dyn PaymentProvider>,
    fault_injector: Arc<FaultInjector>,
    max_wait_ms: i32,
    poll_interval_ms: u64,
    retry_config: payments::consumer::RetryConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let outcome = payments::consumer::process_available(
                &pool,
                consumer.as_ref(),
                producer.as_ref(),
                provider.as_ref(),
                &fault_injector,
                contracts::payments::PAYMENTS_COMMANDS_TOPIC,
                max_wait_ms,
                &retry_config,
            )
            .await;
            match outcome {
                Ok(summary) if summary.records_seen > 0 => {
                    tracing::info!(
                        applied = summary.applied,
                        duplicate = summary.duplicate,
                        stale = summary.stale,
                        poison = summary.poison,
                        "processed payments command batch"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(error = %err, "payments consumer batch failed")
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;
        }
    })
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install ctrl-c handler");
}
