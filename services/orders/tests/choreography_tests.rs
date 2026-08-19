//! M06 acceptance demonstrations (spec section 20): choreography through
//! `PAYMENT_AUTHORIZED` and compensation-matrix rows 1-2 (spec section 12).
//! Real Postgres, real Redpanda. Inventory's and payments' own reactions
//! are simulated by publishing the exact envelopes those services would
//! produce, directly onto `inventory.events.v1`/`payments.events.v1` --
//! this exercises orders' own choreography logic in isolation without
//! needing all three services running as separate processes; a full
//! three-process live run is documented separately in
//! `docs/evidence/m06.md`.

mod common;

use chrono::Utc;
use contracts::Envelope;
use contracts::inventory::ReleaseInventoryPayload;
use contracts::inventory::{
    INVENTORY_COMMANDS_TOPIC, INVENTORY_EVENTS_TOPIC, INVENTORY_RELEASED_EVENT_TYPE,
    INVENTORY_RELEASED_SCHEMA_VERSION, InventoryReleasedPayload, RELEASE_INVENTORY_COMMAND_TYPE,
    RESERVATION_AGGREGATE_TYPE, RESERVATION_FAILED_EVENT_TYPE, RESERVATION_FAILED_SCHEMA_VERSION,
    RESERVATION_SUCCEEDED_EVENT_TYPE, RESERVATION_SUCCEEDED_SCHEMA_VERSION,
    ReservationFailedPayload, ReservationSucceededPayload, ReserveInventoryItem,
};
use contracts::payments::{
    AUTHORIZE_PAYMENT_COMMAND_TYPE, PAYMENT_AGGREGATE_TYPE, PAYMENT_AUTHORIZED_EVENT_TYPE,
    PAYMENT_AUTHORIZED_SCHEMA_VERSION, PAYMENT_FAILED_EVENT_TYPE, PAYMENT_FAILED_SCHEMA_VERSION,
    PAYMENTS_COMMANDS_TOPIC, PAYMENTS_EVENTS_TOPIC, PaymentAuthorizedPayload, PaymentFailedPayload,
};
use orders::config::DeliveryMode;
use orders::domain::OrderStatus;
use serde_json::{Value, json};
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

#[allow(clippy::too_many_arguments)]
fn envelope_bytes<T: serde::Serialize>(
    event_type: &str,
    schema_version: u32,
    producer: &str,
    aggregate_type: &str,
    aggregate_id: Uuid,
    aggregate_version: i64,
    correlation_id: Uuid,
    causation_id: Uuid,
    payload: T,
) -> (Uuid, Vec<u8>) {
    let event_id = Uuid::now_v7();
    let envelope = Envelope {
        event_id,
        event_type: event_type.to_string(),
        schema_version,
        occurred_at: Utc::now(),
        producer: producer.to_string(),
        aggregate_type: aggregate_type.to_string(),
        aggregate_id,
        aggregate_version,
        correlation_id,
        causation_id,
        traceparent: None,
        payload,
    };
    (
        event_id,
        serde_json::to_vec(&envelope).expect("envelope serializes"),
    )
}

/// Creates a real order via the HTTP API under `DeliveryMode::Outbox`,
/// draining its outbox so `order_created` + `reserve_inventory` are
/// actually on the broker, and returns `(order_id, correlation_id,
/// reserve_inventory_event_id)`.
async fn create_order_and_drain(
    pool: &PgPool,
    producer: &dyn messaging::Producer,
) -> (Uuid, Uuid, Uuid) {
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = common::live_state_with_mode(pool.clone(), DeliveryMode::Outbox).await;
    let app: Router = orders::http::router(state);
    let key = format!("choreo-{}", Uuid::now_v7());
    let body = json!({
        "items": [{"sku": "SKU-CHOREO-1", "quantity": 2, "unit_price_minor": 1250}],
        "currency": "USD",
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/orders")
        .header("content-type", "application/json")
        .header("idempotency-key", &key)
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let parsed: Value = serde_json::from_slice(&bytes).unwrap();
    let order_id = Uuid::parse_str(parsed["id"].as_str().unwrap()).unwrap();
    let correlation_id = Uuid::parse_str(parsed["correlation_id"].as_str().unwrap()).unwrap();

    common::drain_outbox(pool, producer).await;

    let reserve_event_id: Uuid =
        sqlx::query_scalar("select id from outbox_events where aggregate_id = $1 and topic = $2")
            .bind(order_id)
            .bind(INVENTORY_COMMANDS_TOPIC)
            .fetch_one(pool)
            .await
            .expect("reserve_inventory outbox row exists");

    (order_id, correlation_id, reserve_event_id)
}

async fn order_status(pool: &PgPool, order_id: Uuid) -> OrderStatus {
    sqlx::query_scalar("select status from orders where id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order exists")
}

/// Happy path (spec section 12): `reservation_succeeded` moves the order to
/// `INVENTORY_RESERVED` and emits `authorize_payment`; `payment_authorized`
/// then moves it to `PAYMENT_AUTHORIZED` -- entirely through
/// events/commands, no synchronous cross-service HTTP call anywhere in
/// this path (M06's own acceptance wording).
#[sqlx::test(migrations = "./migrations")]
async fn happy_path_reaches_payment_authorized(pool: PgPool) {
    let _guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_outcome_offset_to_latest(&pool, &consumer, INVENTORY_EVENTS_TOPIC).await;
    common::seed_outcome_offset_to_latest(&pool, &consumer, PAYMENTS_EVENTS_TOPIC).await;

    let (order_id, correlation_id, reserve_event_id) =
        create_order_and_drain(&pool, &producer).await;
    assert_eq!(order_status(&pool, order_id).await, OrderStatus::Pending);

    let reservation_id = Uuid::now_v7();
    let (_id, bytes) = envelope_bytes(
        RESERVATION_SUCCEEDED_EVENT_TYPE,
        RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        1,
        correlation_id,
        reserve_event_id,
        ReservationSucceededPayload {
            order_id,
            reservation_id,
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 2,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;

    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::InventoryReserved
    );
    let recorded_reservation_id: Uuid =
        sqlx::query_scalar("select reservation_id from orders where id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(recorded_reservation_id, reservation_id);

    common::drain_outbox(&pool, &producer).await;
    let authorize_row: (Uuid, Value) = sqlx::query_as(
        "select id, envelope from outbox_events where aggregate_id = $1 and topic = $2",
    )
    .bind(order_id)
    .bind(PAYMENTS_COMMANDS_TOPIC)
    .fetch_one(&pool)
    .await
    .expect("authorize_payment outbox row exists");
    assert_eq!(
        authorize_row.1["event_type"].as_str().unwrap(),
        AUTHORIZE_PAYMENT_COMMAND_TYPE
    );
    let payment_id =
        Uuid::parse_str(authorize_row.1["payload"]["payment_id"].as_str().unwrap()).unwrap();

    let (_id, bytes) = envelope_bytes(
        PAYMENT_AUTHORIZED_EVENT_TYPE,
        PAYMENT_AUTHORIZED_SCHEMA_VERSION,
        "payments",
        PAYMENT_AGGREGATE_TYPE,
        payment_id,
        1,
        correlation_id,
        authorize_row.0,
        PaymentAuthorizedPayload {
            order_id,
            payment_id,
            provider_reference: "fake-ref-001".to_string(),
        },
    );
    common::publish_raw(
        &producer,
        PAYMENTS_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        PAYMENTS_EVENTS_TOPIC,
    )
    .await;

    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::PaymentAuthorized
    );
}

/// Compensation matrix row 1: inventory rejected -> no payment attempted;
/// cancel -> CANCELLED.
#[sqlx::test(migrations = "./migrations")]
async fn inventory_failure_cancels_with_no_payment_operation(pool: PgPool) {
    let _guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_outcome_offset_to_latest(&pool, &consumer, INVENTORY_EVENTS_TOPIC).await;

    let (order_id, correlation_id, reserve_event_id) =
        create_order_and_drain(&pool, &producer).await;

    let reservation_id = Uuid::now_v7();
    let (_id, bytes) = envelope_bytes(
        RESERVATION_FAILED_EVENT_TYPE,
        RESERVATION_FAILED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        1,
        correlation_id,
        reserve_event_id,
        ReservationFailedPayload {
            order_id,
            reason_code: "INSUFFICIENT_STOCK".to_string(),
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 2,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;

    assert_eq!(order_status(&pool, order_id).await, OrderStatus::Cancelled);

    common::drain_outbox(&pool, &producer).await;
    let payment_command_count: i64 = sqlx::query_scalar(
        "select count(*) from outbox_events where aggregate_id = $1 and topic = $2",
    )
    .bind(order_id)
    .bind(PAYMENTS_COMMANDS_TOPIC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        payment_command_count, 0,
        "no authorize_payment must ever be sent for a rejected reservation"
    );

    let transition_count: i64 =
        sqlx::query_scalar("select count(*) from order_transitions where order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        transition_count, 3,
        "created -> CANCELLING -> CANCELLED, no partial/extra writes"
    );
}

/// Compensation matrix row 2: payment failed after reservation -> release
/// inventory; cancel only after the release is confirmed, not before.
#[sqlx::test(migrations = "./migrations")]
async fn payment_failure_releases_inventory_then_cancels(pool: PgPool) {
    let _guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_outcome_offset_to_latest(&pool, &consumer, INVENTORY_EVENTS_TOPIC).await;
    common::seed_outcome_offset_to_latest(&pool, &consumer, PAYMENTS_EVENTS_TOPIC).await;

    let (order_id, correlation_id, reserve_event_id) =
        create_order_and_drain(&pool, &producer).await;

    let reservation_id = Uuid::now_v7();
    let (_id, bytes) = envelope_bytes(
        RESERVATION_SUCCEEDED_EVENT_TYPE,
        RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        1,
        correlation_id,
        reserve_event_id,
        ReservationSucceededPayload {
            order_id,
            reservation_id,
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 2,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;
    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::InventoryReserved
    );

    let payment_id = Uuid::now_v7();
    let (_id, bytes) = envelope_bytes(
        PAYMENT_FAILED_EVENT_TYPE,
        PAYMENT_FAILED_SCHEMA_VERSION,
        "payments",
        PAYMENT_AGGREGATE_TYPE,
        payment_id,
        1,
        correlation_id,
        reserve_event_id,
        PaymentFailedPayload {
            order_id,
            payment_id,
            reason_code: "CARD_DECLINED".to_string(),
        },
    );
    common::publish_raw(
        &producer,
        PAYMENTS_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        PAYMENTS_EVENTS_TOPIC,
    )
    .await;

    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::Cancelling,
        "must not reach CANCELLED before the release is confirmed"
    );

    common::drain_outbox(&pool, &producer).await;
    let release_row: (Value,) = sqlx::query_as(
        "select envelope from outbox_events where aggregate_id = $1 and topic = $2 \
         order by aggregate_version desc limit 1",
    )
    .bind(order_id)
    .bind(INVENTORY_COMMANDS_TOPIC)
    .fetch_one(&pool)
    .await
    .expect("release_inventory outbox row exists");
    assert_eq!(
        release_row.0["event_type"].as_str().unwrap(),
        RELEASE_INVENTORY_COMMAND_TYPE
    );
    let release_payload: ReleaseInventoryPayload =
        serde_json::from_value(release_row.0["payload"].clone()).unwrap();
    assert_eq!(release_payload.reservation_id, reservation_id);

    let (_id, bytes) = envelope_bytes(
        INVENTORY_RELEASED_EVENT_TYPE,
        INVENTORY_RELEASED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        2,
        correlation_id,
        release_row.0["event_id"].as_str().unwrap().parse().unwrap(),
        InventoryReleasedPayload {
            order_id,
            reservation_id,
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;

    assert_eq!(order_status(&pool, order_id).await, OrderStatus::Cancelled);
}

/// Duplicated and reordered outcomes must not create illegal transitions:
/// `payment_authorized` arriving before `reservation_succeeded` has been
/// processed (order still PENDING) is rejected as an illegal transition and
/// logged, not applied or crashed on; redelivering the same
/// `reservation_succeeded` event is recognized as a duplicate.
#[sqlx::test(migrations = "./migrations")]
async fn duplicated_and_reordered_outcomes_do_not_create_illegal_transitions(pool: PgPool) {
    let _guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_outcome_offset_to_latest(&pool, &consumer, INVENTORY_EVENTS_TOPIC).await;
    common::seed_outcome_offset_to_latest(&pool, &consumer, PAYMENTS_EVENTS_TOPIC).await;

    let (order_id, correlation_id, reserve_event_id) =
        create_order_and_drain(&pool, &producer).await;

    // Reordered: payment_authorized arrives while the order is still
    // PENDING (INVENTORY_RESERVED->PAYMENT_AUTHORIZED never happened yet).
    let payment_id = Uuid::now_v7();
    let (_id, bytes) = envelope_bytes(
        PAYMENT_AUTHORIZED_EVENT_TYPE,
        PAYMENT_AUTHORIZED_SCHEMA_VERSION,
        "payments",
        PAYMENT_AGGREGATE_TYPE,
        payment_id,
        1,
        correlation_id,
        reserve_event_id,
        PaymentAuthorizedPayload {
            order_id,
            payment_id,
            provider_reference: "fake-ref-002".to_string(),
        },
    );
    common::publish_raw(
        &producer,
        PAYMENTS_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    let summary = common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        PAYMENTS_EVENTS_TOPIC,
    )
    .await;
    assert_eq!(
        summary.applied, 1,
        "the record is consumed (inbox-marked) even though the transition itself is a no-op"
    );
    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::Pending,
        "an out-of-order payment_authorized must not force an illegal PENDING->PAYMENT_AUTHORIZED transition"
    );

    // Now the real order: reservation_succeeded, delivered twice (a raw
    // redelivery -- identical bytes, same event_id).
    let reservation_id = Uuid::now_v7();
    let (_id, bytes) = envelope_bytes(
        RESERVATION_SUCCEEDED_EVENT_TYPE,
        RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        1,
        correlation_id,
        reserve_event_id,
        ReservationSucceededPayload {
            order_id,
            reservation_id,
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 2,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes.clone(),
    )
    .await;
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    let summary = common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;
    assert_eq!(summary.applied, 1);
    assert_eq!(summary.duplicate, 1);
    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::InventoryReserved
    );
}

/// An outcome record naming an order this consumer's database has no row
/// for (e.g. a genuinely unknown order, or -- as this exact scenario was
/// first caught live -- cross-talk on a shared dev broker) must reach the
/// DLQ and let the partition keep moving, not propagate as an
/// infrastructure error that wedges the offset ledger on that record
/// forever (invariant I15). Regression test for a real bug this milestone's
/// own live end-to-end check surfaced (see `docs/evidence/m06.md`).
#[sqlx::test(migrations = "./migrations")]
async fn unknown_order_reference_does_not_block_the_partition(pool: PgPool) {
    let _guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_outcome_offset_to_latest(&pool, &consumer, INVENTORY_EVENTS_TOPIC).await;

    let unknown_order_id = Uuid::now_v7();
    let unknown_reservation_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let (_id, poison_bytes) = envelope_bytes(
        RESERVATION_SUCCEEDED_EVENT_TYPE,
        RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        unknown_reservation_id,
        1,
        correlation_id,
        Uuid::now_v7(),
        ReservationSucceededPayload {
            order_id: unknown_order_id,
            reservation_id: unknown_reservation_id,
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 1,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &unknown_order_id.to_string(),
        poison_bytes,
    )
    .await;

    // A real, valid order right behind the poison record on the same
    // topic -- it must still be processed.
    let (order_id, real_correlation_id, reserve_event_id) =
        create_order_and_drain(&pool, &producer).await;
    let reservation_id = Uuid::now_v7();
    let (_id, valid_bytes) = envelope_bytes(
        RESERVATION_SUCCEEDED_EVENT_TYPE,
        RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        1,
        real_correlation_id,
        reserve_event_id,
        ReservationSucceededPayload {
            order_id,
            reservation_id,
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 2,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        valid_bytes,
    )
    .await;

    let summary = common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;
    assert!(
        !summary.stopped_by_fault,
        "the unknown-order record must not stop the batch"
    );
    assert_eq!(summary.poison, 1, "the unknown-order record is DLQ'd");
    assert_eq!(
        summary.applied, 1,
        "the real order right behind it still applies"
    );
    assert_eq!(
        order_status(&pool, order_id).await,
        OrderStatus::InventoryReserved,
        "the valid record must not be blocked by the poison one ahead of it"
    );
}

/// Every event/command in one order's journey shares its correlation_id,
/// and each one's causation_id points at the record that triggered it
/// (spec section 16).
#[sqlx::test(migrations = "./migrations")]
async fn correlation_and_causation_chain_is_complete(pool: PgPool) {
    let _guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_outcome_offset_to_latest(&pool, &consumer, INVENTORY_EVENTS_TOPIC).await;

    let (order_id, correlation_id, reserve_event_id) =
        create_order_and_drain(&pool, &producer).await;

    let order_created_row: (Value,) = sqlx::query_as(
        "select envelope from outbox_events where aggregate_id = $1 and topic = 'orders.events.v1'",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        order_created_row.0["correlation_id"].as_str().unwrap(),
        correlation_id.to_string()
    );

    let reserve_row: (Value,) =
        sqlx::query_as("select envelope from outbox_events where aggregate_id = $1 and topic = $2")
            .bind(order_id)
            .bind(INVENTORY_COMMANDS_TOPIC)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        reserve_row.0["correlation_id"].as_str().unwrap(),
        correlation_id.to_string()
    );
    assert_eq!(
        reserve_row.0["event_id"].as_str().unwrap(),
        reserve_event_id.to_string()
    );

    let reservation_id = Uuid::now_v7();
    let (reservation_event_id, bytes) = envelope_bytes(
        RESERVATION_SUCCEEDED_EVENT_TYPE,
        RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        "inventory",
        RESERVATION_AGGREGATE_TYPE,
        reservation_id,
        1,
        correlation_id,
        reserve_event_id,
        ReservationSucceededPayload {
            order_id,
            reservation_id,
            items: vec![ReserveInventoryItem {
                sku: "SKU-CHOREO-1".to_string(),
                quantity: 2,
            }],
        },
    );
    common::publish_raw(
        &producer,
        INVENTORY_EVENTS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;
    common::drain_outcomes(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_EVENTS_TOPIC,
    )
    .await;
    common::drain_outbox(&pool, &producer).await;

    let authorize_row: (Value,) =
        sqlx::query_as("select envelope from outbox_events where aggregate_id = $1 and topic = $2")
            .bind(order_id)
            .bind(PAYMENTS_COMMANDS_TOPIC)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        authorize_row.0["correlation_id"].as_str().unwrap(),
        correlation_id.to_string(),
        "correlation_id propagates across the whole chain"
    );
    assert_eq!(
        authorize_row.0["causation_id"].as_str().unwrap(),
        reservation_event_id.to_string(),
        "causation_id points at the exact event that triggered this command"
    );
}
