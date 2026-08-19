use std::sync::Arc;

use fulfilment::config::{Config, DatabaseConfig, FailureInjectionConfig, MessagingConfig};
use fulfilment::consumer;
use fulfilment::http::{self, AppState};
use messaging::{RskafkaConsumer, RskafkaProducer};
use persistence::outbox::PublishMetrics;
use sqlx::postgres::PgPoolOptions;
use test_support::FaultInjector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing("fulfilment");

    let config = Config::load()?;
    let database = DatabaseConfig::load()?;
    let messaging = MessagingConfig::load()?;
    let failure_injection = FailureInjectionConfig::load()?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database.database_url())
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let producer: Arc<dyn messaging::Producer> =
        Arc::new(RskafkaProducer::connect(vec![messaging.redpanda_broker.clone()]).await?);
    let consumer: Arc<dyn messaging::Consumer> =
        Arc::new(RskafkaConsumer::connect(vec![messaging.redpanda_broker.clone()]).await?);
    let fault_injector = Arc::new(FaultInjector::new());
    let publish_metrics = Arc::new(PublishMetrics::default());

    let publisher_config = config.publisher_config(format!("fulfilment-{}", std::process::id()));
    let publisher_handle = persistence::outbox::spawn_publisher_loop(
        pool.clone(),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        publisher_config,
        Arc::clone(&publish_metrics),
    );

    let consumer_handle = spawn_consumer_loop(
        pool.clone(),
        Arc::clone(&consumer),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        config.consumer_max_wait_ms,
        config.consumer_poll_interval_ms,
        config.retry_config(),
    );

    let state = AppState {
        pool,
        fault_injector,
        failure_injection_enabled: failure_injection.enabled(),
        failure_injection_token: failure_injection.failure_injection_token,
        publish_metrics,
    };
    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(addr = %config.socket_addr(), "fulfilment listening");

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    publisher_handle.abort();
    consumer_handle.abort();

    Ok(())
}

fn spawn_consumer_loop(
    pool: sqlx::PgPool,
    consumer: Arc<dyn messaging::Consumer>,
    producer: Arc<dyn messaging::Producer>,
    fault_injector: Arc<FaultInjector>,
    max_wait_ms: i32,
    poll_interval_ms: u64,
    retry_config: consumer::RetryConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match consumer::process_available(
                &pool,
                consumer.as_ref(),
                producer.as_ref(),
                fault_injector.as_ref(),
                contracts::fulfilment::FULFILMENT_COMMANDS_TOPIC,
                max_wait_ms,
                &retry_config,
            )
            .await
            {
                Ok(summary) if summary.records_seen > 0 => {
                    tracing::info!(?summary, "fulfilment command batch processed");
                }
                Ok(_) => {}
                Err(error) => tracing::error!(%error, "fulfilment consumer poll failed"),
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
