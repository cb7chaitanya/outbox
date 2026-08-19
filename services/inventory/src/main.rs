use std::sync::Arc;

use inventory::config::{Config, DatabaseConfig, FailureInjectionConfig, MessagingConfig};
use inventory::http::{self, AppState};
use messaging::{RskafkaConsumer, RskafkaProducer};
use persistence::outbox::PublishMetrics;
use sqlx::postgres::PgPoolOptions;
use test_support::FaultInjector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing("inventory");

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
    let publish_metrics = Arc::new(PublishMetrics::default());

    let publisher_claimed_by = format!("inventory-{}", uuid::Uuid::now_v7());
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
        Arc::clone(&fault_injector),
        config.consumer_max_wait_ms,
        config.consumer_poll_interval_ms,
    );

    let state = AppState {
        pool,
        fault_injector,
        failure_injection_enabled: failure_injection_config.enabled(),
        failure_injection_token: failure_injection_config.failure_injection_token.clone(),
        publish_metrics,
    };

    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(addr = %config.socket_addr(), "inventory listening");

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    publisher_handle.abort();
    consumer_handle.abort();

    Ok(())
}

/// Polls `inventory.commands.v1` forever, running the eight-step inbox
/// protocol (`inventory::consumer::process_available`) on whatever's new
/// each tick.
fn spawn_consumer_loop(
    pool: sqlx::PgPool,
    consumer: Arc<dyn messaging::Consumer>,
    producer: Arc<dyn messaging::Producer>,
    fault_injector: Arc<FaultInjector>,
    max_wait_ms: i32,
    poll_interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let outcome = inventory::consumer::process_available(
                &pool,
                consumer.as_ref(),
                producer.as_ref(),
                &fault_injector,
                contracts::inventory::INVENTORY_COMMANDS_TOPIC,
                max_wait_ms,
            )
            .await;
            match outcome {
                Ok(summary) if summary.records_seen > 0 => {
                    tracing::info!(
                        applied = summary.applied,
                        duplicate = summary.duplicate,
                        stale = summary.stale,
                        poison = summary.poison,
                        "processed reserve_inventory batch"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(error = %err, "reserve_inventory consumer batch failed")
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
