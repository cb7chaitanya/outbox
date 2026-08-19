use std::sync::Arc;

use messaging::{RskafkaConsumer, RskafkaProducer};
use orders::config::{Config, DatabaseConfig, FailureInjectionConfig, MessagingConfig};
use orders::http::{self, AppState};
use persistence::outbox::PublishMetrics;
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

    let producer: Arc<dyn messaging::Producer> =
        Arc::new(RskafkaProducer::connect(vec![messaging_config.redpanda_broker.clone()]).await?);
    let consumer: Arc<dyn messaging::Consumer> =
        Arc::new(RskafkaConsumer::connect(vec![messaging_config.redpanda_broker.clone()]).await?);

    if failure_injection_config.enabled() {
        tracing::warn!("FAILURE_INJECTION_ENABLED=true: /_test/faults/* is mounted");
    }

    let fault_injector = Arc::new(FaultInjector::new());
    let publish_metrics = Arc::new(PublishMetrics::default());

    // The outbox publisher runs regardless of `delivery_mode`: in `naive`
    // mode the table simply stays empty, so the loop is a harmless no-op
    // poll. Running it unconditionally means flipping `DELIVERY_MODE` back
    // to `outbox` never requires a restart-time wiring change.
    let publisher_claimed_by = format!("orders-{}", uuid::Uuid::now_v7());
    let publisher_handle = persistence::outbox::spawn_publisher_loop(
        pool.clone(),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        config.publisher_config(publisher_claimed_by),
        Arc::clone(&publish_metrics),
    );

    let inventory_outcome_consumer_handle = spawn_outcome_consumer_loop(
        pool.clone(),
        Arc::clone(&consumer),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        contracts::inventory::INVENTORY_EVENTS_TOPIC,
        config.consumer_max_wait_ms,
        config.consumer_poll_interval_ms,
    );
    let payment_outcome_consumer_handle = spawn_outcome_consumer_loop(
        pool.clone(),
        Arc::clone(&consumer),
        Arc::clone(&producer),
        Arc::clone(&fault_injector),
        contracts::payments::PAYMENTS_EVENTS_TOPIC,
        config.consumer_max_wait_ms,
        config.consumer_poll_interval_ms,
    );

    let state = AppState {
        pool,
        producer,
        fault_injector,
        delivery_mode: config.delivery_mode,
        failure_injection_enabled: failure_injection_config.enabled(),
        failure_injection_token: failure_injection_config.failure_injection_token.clone(),
        publish_metrics,
    };

    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(addr = %config.socket_addr(), "orders listening");

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    publisher_handle.abort();
    inventory_outcome_consumer_handle.abort();
    payment_outcome_consumer_handle.abort();

    Ok(())
}

/// Polls `source_topic` forever, running the idempotent-inbox protocol
/// (`orders::outcome_consumer::process_available`) on whatever's new each
/// tick (spec section 12: react to reservation and payment outcomes). One
/// loop instance per topic — `main` spawns one for `inventory.events.v1`
/// and one for `payments.events.v1`.
fn spawn_outcome_consumer_loop(
    pool: sqlx::PgPool,
    consumer: Arc<dyn messaging::Consumer>,
    producer: Arc<dyn messaging::Producer>,
    fault_injector: Arc<FaultInjector>,
    source_topic: &'static str,
    max_wait_ms: i32,
    poll_interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let outcome = orders::outcome_consumer::process_available(
                &pool,
                consumer.as_ref(),
                producer.as_ref(),
                &fault_injector,
                source_topic,
                max_wait_ms,
            )
            .await;
            match outcome {
                Ok(summary) if summary.records_seen > 0 => {
                    tracing::info!(
                        topic = source_topic,
                        applied = summary.applied,
                        duplicate = summary.duplicate,
                        stale = summary.stale,
                        poison = summary.poison,
                        "processed outcome batch"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(topic = source_topic, error = %err, "outcome consumer batch failed")
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
