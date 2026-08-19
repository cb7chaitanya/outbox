//! M05 acceptance demonstrations (spec section 20) against a real Postgres
//! database and a real Redpanda broker.

mod common;

use std::sync::Arc;

use contracts::payments::PAYMENTS_COMMANDS_TOPIC;
use messaging::Consumer as _;
use payments::domain::{authorize_idempotency_key, refund_idempotency_key};
use payments::provider::{
    FAULT_PROVIDER_DECLINE, FAULT_PROVIDER_RESPONSE_LOST, FAULT_PROVIDER_TIMEOUT,
};
use sqlx::PgPool;
use test_support::{FaultConfig, FaultInjector};
use uuid::Uuid;

/// "Timeout then success authorizes once" (M05 acceptance): a provider
/// timeout on the first attempt, success on the retry, must leave exactly
/// one `AUTHORIZED` payment row and one `payment_authorized` outbox event.
#[sqlx::test(migrations = "./migrations")]
async fn timeout_then_success_authorizes_once(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    common::seed_offset_to_latest(&pool, &consumer, PAYMENTS_COMMANDS_TOPIC).await;

    let fault_injector = Arc::new(FaultInjector::new());
    let provider = common::fake_provider(Arc::clone(&fault_injector));

    let order_id = Uuid::now_v7();
    let payment_id = Uuid::now_v7();
    let idempotency_key = authorize_idempotency_key(order_id);
    fault_injector.configure(
        FAULT_PROVIDER_TIMEOUT,
        FaultConfig {
            fail_next: 1,
            subject_filter: Some(idempotency_key),
            delay_ms: None,
        },
    );

    let (_event_id, bytes) =
        common::build_authorize_envelope(order_id, payment_id, 2500, "USD", 1, Uuid::now_v7());
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;

    let summary = common::drain(
        &pool,
        &consumer,
        &producer,
        &provider,
        &fault_injector,
        PAYMENTS_COMMANDS_TOPIC,
        &common::fast_retry_config(),
    )
    .await;

    assert_eq!(summary.applied, 1);
    assert_eq!(summary.poison, 0);
    assert_eq!(provider.real_authorize_calls(), 1);

    let authorized_count: i64 = sqlx::query_scalar(
        "select count(*) from payments where order_id = $1 and status = 'AUTHORIZED'",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(authorized_count, 1);

    let event_count: i64 = sqlx::query_scalar(
        "select count(*) from outbox_events where message_key = $1 and envelope->>'event_type' = 'payments.payment_authorized'",
    )
    .bind(order_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1);
}

/// "Lost success response followed by retry creates one provider
/// operation" (M05 acceptance): the provider commits the authorization but
/// the response is lost; the handler's retry with the same idempotency key
/// must hit the provider's cached result, not perform a second real
/// authorization.
#[sqlx::test(migrations = "./migrations")]
async fn lost_success_response_then_retry_creates_one_provider_operation(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    common::seed_offset_to_latest(&pool, &consumer, PAYMENTS_COMMANDS_TOPIC).await;

    let fault_injector = Arc::new(FaultInjector::new());
    let provider = common::fake_provider(Arc::clone(&fault_injector));

    let order_id = Uuid::now_v7();
    let payment_id = Uuid::now_v7();
    let idempotency_key = authorize_idempotency_key(order_id);
    fault_injector.configure(
        FAULT_PROVIDER_RESPONSE_LOST,
        FaultConfig {
            fail_next: 1,
            subject_filter: Some(idempotency_key),
            delay_ms: None,
        },
    );

    let (_event_id, bytes) =
        common::build_authorize_envelope(order_id, payment_id, 4200, "USD", 1, Uuid::now_v7());
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;

    let summary = common::drain(
        &pool,
        &consumer,
        &producer,
        &provider,
        &fault_injector,
        PAYMENTS_COMMANDS_TOPIC,
        &common::fast_retry_config(),
    )
    .await;

    assert_eq!(summary.applied, 1);
    assert_eq!(
        provider.real_authorize_calls(),
        1,
        "the lost-response call already committed the result; the in-handler retry must be a cache hit, not a second real operation"
    );

    let authorized_count: i64 = sqlx::query_scalar(
        "select count(*) from payments where order_id = $1 and status = 'AUTHORIZED'",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(authorized_count, 1);
}

/// "Decline emits business failure without DLQ retry storm" (M05
/// acceptance): a decline is a business outcome, not an error — one
/// `payment_failed` event, no retries, nothing DLQ'd.
#[sqlx::test(migrations = "./migrations")]
async fn decline_emits_business_failure_without_dlq_retry_storm(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    common::seed_offset_to_latest(&pool, &consumer, PAYMENTS_COMMANDS_TOPIC).await;

    let fault_injector = Arc::new(FaultInjector::new());
    let provider = common::fake_provider(Arc::clone(&fault_injector));

    let order_id = Uuid::now_v7();
    let payment_id = Uuid::now_v7();
    let idempotency_key = authorize_idempotency_key(order_id);
    fault_injector.configure(
        FAULT_PROVIDER_DECLINE,
        FaultConfig {
            fail_next: 1,
            subject_filter: Some(idempotency_key),
            delay_ms: None,
        },
    );

    let (_event_id, bytes) =
        common::build_authorize_envelope(order_id, payment_id, 999, "USD", 1, Uuid::now_v7());
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;

    let summary = common::drain(
        &pool,
        &consumer,
        &producer,
        &provider,
        &fault_injector,
        PAYMENTS_COMMANDS_TOPIC,
        &common::fast_retry_config(),
    )
    .await;

    assert_eq!(summary.applied, 1);
    assert_eq!(summary.poison, 0, "a business decline must never be DLQ'd");
    assert_eq!(
        provider.real_authorize_calls(),
        1,
        "a decline is a final answer, not a retry trigger"
    );

    let failed_count: i64 = sqlx::query_scalar(
        "select count(*) from payments where order_id = $1 and status = 'FAILED'",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(failed_count, 1);

    let event_count: i64 = sqlx::query_scalar(
        "select count(*) from outbox_events where message_key = $1 and envelope->>'event_type' = 'payments.payment_failed'",
    )
    .bind(order_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1);
}

/// "Poison input reaches DLQ; retry metrics/error codes are correct" (M05
/// acceptance): a malformed record is dead-lettered without blocking a
/// valid command that follows it (invariant I15).
#[sqlx::test(migrations = "./migrations")]
async fn poison_input_reaches_dlq_without_blocking_valid_work(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    common::seed_offset_to_latest(&pool, &consumer, PAYMENTS_COMMANDS_TOPIC).await;
    let dlq_topic = persistence::dlq::dlq_topic(PAYMENTS_COMMANDS_TOPIC);
    let dlq_start_offset = consumer.latest_offset(&dlq_topic).await.unwrap();

    let fault_injector = Arc::new(FaultInjector::new());
    let provider = common::fake_provider(Arc::clone(&fault_injector));

    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        "poison-key",
        b"not valid json at all".to_vec(),
    )
    .await;

    let order_id = Uuid::now_v7();
    let payment_id = Uuid::now_v7();
    let (_event_id, bytes) =
        common::build_authorize_envelope(order_id, payment_id, 1500, "USD", 1, Uuid::now_v7());
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;

    let summary = common::drain(
        &pool,
        &consumer,
        &producer,
        &provider,
        &fault_injector,
        PAYMENTS_COMMANDS_TOPIC,
        &common::fast_retry_config(),
    )
    .await;

    assert_eq!(
        summary.poison, 1,
        "the malformed record must be dead-lettered"
    );
    assert_eq!(
        summary.applied, 1,
        "the valid command after it must still process normally"
    );

    let dlq_records = consumer
        .fetch(&dlq_topic, dlq_start_offset, 5_000)
        .await
        .expect("fetch dlq records");
    assert_eq!(dlq_records.len(), 1, "exactly one record reached the DLQ");

    let authorized_count: i64 = sqlx::query_scalar(
        "select count(*) from payments where order_id = $1 and status = 'AUTHORIZED'",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(authorized_count, 1);
}

/// "Refund is idempotent" (M05 acceptance): redelivering the exact same
/// `refund_payment` command must produce exactly one real refund effect
/// and one `payment_refunded` event.
#[sqlx::test(migrations = "./migrations")]
async fn refund_is_idempotent(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    common::seed_offset_to_latest(&pool, &consumer, PAYMENTS_COMMANDS_TOPIC).await;

    let fault_injector = Arc::new(FaultInjector::new());
    let provider = common::fake_provider(Arc::clone(&fault_injector));

    let order_id = Uuid::now_v7();
    let payment_id = Uuid::now_v7();

    let (_authorize_event_id, authorize_bytes) =
        common::build_authorize_envelope(order_id, payment_id, 3000, "USD", 1, Uuid::now_v7());
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        authorize_bytes,
    )
    .await;
    let after_authorize = common::drain(
        &pool,
        &consumer,
        &producer,
        &provider,
        &fault_injector,
        PAYMENTS_COMMANDS_TOPIC,
        &common::fast_retry_config(),
    )
    .await;
    assert_eq!(after_authorize.applied, 1);

    let (_refund_event_id, refund_bytes) =
        common::build_refund_envelope(order_id, payment_id, 2, Uuid::now_v7());
    // Two deliveries of the exact same refund event — a real broker
    // redelivery, not two distinct requests.
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        refund_bytes.clone(),
    )
    .await;
    common::publish_raw(
        &producer,
        PAYMENTS_COMMANDS_TOPIC,
        &order_id.to_string(),
        refund_bytes,
    )
    .await;

    let after_refund = common::drain(
        &pool,
        &consumer,
        &producer,
        &provider,
        &fault_injector,
        PAYMENTS_COMMANDS_TOPIC,
        &common::fast_retry_config(),
    )
    .await;
    assert_eq!(
        after_refund.applied, 1,
        "only the first refund delivery applies"
    );
    assert_eq!(
        after_refund.duplicate, 1,
        "the redelivery must be recognized as a duplicate"
    );
    assert_eq!(provider.real_refund_calls(), 1);

    let refunded_count: i64 = sqlx::query_scalar(
        "select count(*) from payments where order_id = $1 and status = 'REFUNDED'",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(refunded_count, 1);

    let event_count: i64 = sqlx::query_scalar(
        "select count(*) from outbox_events where message_key = $1 and envelope->>'event_type' = 'payments.payment_refunded'",
    )
    .bind(order_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1);

    // The `refund_idempotency_key` derivation itself is stable per order,
    // which is what makes the provider-ledger side of this idempotent too.
    let _ = refund_idempotency_key(order_id);
}
