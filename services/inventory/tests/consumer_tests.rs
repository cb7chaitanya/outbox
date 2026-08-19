//! M04 acceptance demonstrations (spec section 20) against a real
//! Postgres database and a real Redpanda broker.

mod common;

use chrono::Utc;
use contracts::inventory::INVENTORY_COMMANDS_TOPIC;
use inventory::consumer::FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT;
use inventory::domain::ReservationStatus;
use inventory::repository;
use messaging::Consumer as _;
use sqlx::PgPool;
use test_support::{FaultConfig, FaultInjector};
use uuid::Uuid;

/// Duplicate `reserve_inventory` delivery (the same event, redelivered —
/// spec section 14's at-least-once transport) must create exactly one
/// reservation and publish exactly one outcome event.
#[sqlx::test(migrations = "./migrations")]
async fn duplicate_delivery_creates_one_reservation(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_offset_to_latest(&pool, &consumer, INVENTORY_COMMANDS_TOPIC).await;

    repository::seed_stock(&pool, "SKU-DUP-1", 10, Utc::now())
        .await
        .expect("seed stock");

    let order_id = Uuid::now_v7();
    let (_event_id, bytes) =
        common::build_reserve_envelope(order_id, 1, &[("SKU-DUP-1", 2)], Uuid::now_v7());

    // Two deliveries of the exact same event (identical bytes, same
    // event_id) — a real broker redelivery, not two distinct requests.
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes.clone(),
    )
    .await;
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes,
    )
    .await;

    let summary = common::drain(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_COMMANDS_TOPIC,
    )
    .await;
    assert_eq!(summary.records_seen, 2);
    assert_eq!(summary.applied, 1, "only the first delivery should apply");
    assert_eq!(
        summary.duplicate, 1,
        "the redelivery must be recognized as a duplicate"
    );

    let reservation_count: i64 =
        sqlx::query_scalar("select count(*) from reservations where order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reservation_count, 1);

    let outbox_count: i64 =
        sqlx::query_scalar("select count(*) from outbox_events where message_key = $1")
            .bind(order_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        outbox_count, 1,
        "exactly one outcome event, not one per delivery"
    );
}

/// Concurrent reservations against the same SKU must never oversell:
/// `available + reserved = initial` always holds, and `available_qty`
/// never goes negative (invariant I5).
#[sqlx::test(migrations = "./migrations")]
async fn concurrent_reservations_never_oversell(pool: PgPool) {
    repository::seed_stock(&pool, "SKU-CONTENDED", 5, Utc::now())
        .await
        .expect("seed stock");

    let mut handles = Vec::new();
    for _ in 0..10 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let order_id = Uuid::now_v7();
            let lines = inventory::domain::validate_items(&[("SKU-CONTENDED".to_string(), 1)])
                .expect("valid request");
            let mut tx = pool.begin().await.expect("begin");
            let outcome = repository::reserve(&mut tx, order_id, &lines, Utc::now())
                .await
                .expect("reserve does not error");
            tx.commit().await.expect("commit");
            outcome.status
        }));
    }

    let mut succeeded = 0;
    let mut rejected = 0;
    for handle in handles {
        match handle.await.unwrap() {
            ReservationStatus::Active => succeeded += 1,
            ReservationStatus::Rejected => rejected += 1,
            other => panic!("unexpected status {other:?}"),
        }
    }

    assert_eq!(succeeded, 5, "only 5 units were available");
    assert_eq!(rejected, 5);

    let stock = repository::get_stock(&pool, "SKU-CONTENDED")
        .await
        .unwrap()
        .expect("stock row exists");
    assert_eq!(stock.available_qty, 0);
    assert_eq!(stock.reserved_qty, 5);
    assert!(stock.available_qty >= 0);
    assert_eq!(stock.available_qty + stock.reserved_qty, 5);
}

/// A multi-SKU reservation where one SKU lacks sufficient stock must
/// reserve nothing — not even the SKUs that had enough (all-or-nothing).
#[sqlx::test(migrations = "./migrations")]
async fn multi_sku_reservation_is_all_or_nothing(pool: PgPool) {
    repository::seed_stock(&pool, "SKU-PLENTY", 100, Utc::now())
        .await
        .expect("seed plenty");
    repository::seed_stock(&pool, "SKU-SCARCE", 1, Utc::now())
        .await
        .expect("seed scarce");

    let order_id = Uuid::now_v7();
    let lines = inventory::domain::validate_items(&[
        ("SKU-PLENTY".to_string(), 5),
        ("SKU-SCARCE".to_string(), 10),
    ])
    .expect("valid request");

    let mut tx = pool.begin().await.unwrap();
    let outcome = repository::reserve(&mut tx, order_id, &lines, Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(outcome.status, ReservationStatus::Rejected);
    assert_eq!(outcome.reason_code.as_deref(), Some("INSUFFICIENT_STOCK"));

    let plenty = repository::get_stock(&pool, "SKU-PLENTY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        plenty.available_qty, 100,
        "the sufficient SKU must not be touched when the reservation is rejected"
    );
    assert_eq!(plenty.reserved_qty, 0);
}

/// A crash simulated between the handler's DB commit and this project's
/// offset-ledger commit must not cause a duplicate business effect on
/// redelivery: the inbox row committed inside the business transaction
/// already makes the redelivered record a no-op (spec section 14).
#[sqlx::test(migrations = "./migrations")]
async fn crash_after_db_commit_before_offset_commit_has_no_duplicate_effect(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_offset_to_latest(&pool, &consumer, INVENTORY_COMMANDS_TOPIC).await;

    repository::seed_stock(&pool, "SKU-CRASH", 10, Utc::now())
        .await
        .expect("seed stock");

    let order_id = Uuid::now_v7();
    common::publish_reserve_command(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        order_id,
        1,
        &[("SKU-CRASH", 3)],
        Uuid::now_v7(),
    )
    .await;

    fault_injector.configure(
        FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT,
        FaultConfig {
            fail_next: 1,
            subject_filter: None,
            delay_ms: None,
        },
    );

    // First pass: the business transaction commits (reservation created),
    // then the fault fires before the offset ledger advances.
    let first = common::drain(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_COMMANDS_TOPIC,
    )
    .await;
    assert!(first.stopped_by_fault, "fault must have fired");
    assert_eq!(first.applied, 1);

    let reservation_count: i64 =
        sqlx::query_scalar("select count(*) from reservations where order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        reservation_count, 1,
        "the business effect already committed"
    );

    let stock_after_first = repository::get_stock(&pool, "SKU-CRASH")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stock_after_first.reserved_qty, 3);

    // Second pass (simulating a restart): the fault is gone, the offset
    // ledger is unchanged, so the same record is redelivered. The inbox
    // row from the first pass must make this a no-op.
    let second = common::drain(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_COMMANDS_TOPIC,
    )
    .await;
    assert_eq!(
        second.duplicate, 1,
        "redelivery must be recognized, not reapplied"
    );
    assert_eq!(second.applied, 0);

    let reservation_count_after: i64 =
        sqlx::query_scalar("select count(*) from reservations where order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reservation_count_after, 1, "still exactly one reservation");

    let stock_after_second = repository::get_stock(&pool, "SKU-CRASH")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stock_after_second.reserved_qty, 3,
        "stock must not be decremented twice"
    );
}

/// An envelope with an unsupported schema version reaches the DLQ, and a
/// subsequent valid message still processes normally — poison isolation
/// (invariant I15).
#[sqlx::test(migrations = "./migrations")]
async fn invalid_schema_reaches_dlq_without_blocking_valid_work(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_offset_to_latest(&pool, &consumer, INVENTORY_COMMANDS_TOPIC).await;

    repository::seed_stock(&pool, "SKU-POISON", 10, Utc::now())
        .await
        .expect("seed stock");

    // A structurally-valid envelope, but with a schema_version this
    // consumer does not understand.
    let poison_order_id = Uuid::now_v7();
    let bad_envelope = serde_json::json!({
        "event_id": Uuid::now_v7(),
        "event_type": "inventory.reserve_inventory",
        "schema_version": 999,
        "occurred_at": Utc::now(),
        "producer": "orders",
        "aggregate_type": "order",
        "aggregate_id": poison_order_id,
        "aggregate_version": 1,
        "correlation_id": Uuid::now_v7(),
        "causation_id": Uuid::now_v7(),
        "payload": { "order_id": poison_order_id, "items": [], "expected_order_version": 1 },
    });
    assert_eq!(bad_envelope["schema_version"], 999);
    let bad_bytes = serde_json::to_vec(&bad_envelope).unwrap();
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &poison_order_id.to_string(),
        bad_bytes,
    )
    .await;

    let dlq_topic = persistence::dlq::dlq_topic(INVENTORY_COMMANDS_TOPIC);
    let dlq_offset_before_test = consumer.latest_offset(&dlq_topic).await.unwrap();

    let good_order_id = Uuid::now_v7();
    common::publish_reserve_command(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        good_order_id,
        1,
        &[("SKU-POISON", 1)],
        Uuid::now_v7(),
    )
    .await;

    let summary = common::drain(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_COMMANDS_TOPIC,
    )
    .await;
    assert_eq!(
        summary.poison, 1,
        "the unsupported-schema message must be dead-lettered"
    );
    assert_eq!(
        summary.applied, 1,
        "the valid message on the same partition must still process"
    );

    let good_reservation: i64 =
        sqlx::query_scalar("select count(*) from reservations where order_id = $1")
            .bind(good_order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(good_reservation, 1);

    let poison_reservation: i64 =
        sqlx::query_scalar("select count(*) from reservations where order_id = $1")
            .bind(poison_order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        poison_reservation, 0,
        "the poison message must not create a reservation"
    );

    // Verify the DLQ record actually landed on the broker with the
    // expected error code (only the record(s) this test itself produced,
    // reading from the offset captured before this test published).
    let records = consumer
        .fetch(&dlq_topic, dlq_offset_before_test, 1_000)
        .await
        .unwrap();
    assert!(
        !records.is_empty(),
        "dlq topic must have a new record from this test"
    );
    let found = records.iter().any(|r| {
        r.value
            .as_deref()
            .and_then(|v| serde_json::from_slice::<serde_json::Value>(v).ok())
            .map(|v| v["error_code"] == "UNSUPPORTED_SCHEMA")
            .unwrap_or(false)
    });
    assert!(
        found,
        "expected an UNSUPPORTED_SCHEMA record on the dlq topic"
    );
}
