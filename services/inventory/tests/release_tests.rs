//! M06 compensation-matrix row 2 (spec section 12): inventory's half of
//! "payment failed after reservation -> release inventory". Real Postgres,
//! real Redpanda.

mod common;

use chrono::Utc;
use contracts::inventory::{INVENTORY_COMMANDS_TOPIC, INVENTORY_EVENTS_TOPIC};
use inventory::domain::ReservationStatus;
use inventory::repository;
use messaging::Consumer as _;
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

/// Releasing an active reservation restores `available_qty`/`reserved_qty`
/// (invariant I5) and publishes exactly one `inventory_released` event.
#[sqlx::test(migrations = "./migrations")]
async fn release_restores_stock_and_publishes_released_event(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_offset_to_latest(&pool, &consumer, INVENTORY_COMMANDS_TOPIC).await;

    repository::seed_stock(&pool, "SKU-REL-1", 10, Utc::now())
        .await
        .expect("seed stock");

    let order_id = Uuid::now_v7();
    let (_reserve_event_id, reserve_bytes) =
        common::build_reserve_envelope(order_id, 1, &[("SKU-REL-1", 3)], Uuid::now_v7());
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        reserve_bytes,
    )
    .await;
    common::drain(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_COMMANDS_TOPIC,
    )
    .await;
    common::drain_outbox(&pool, &producer).await;

    let after_reserve = repository::get_stock(&pool, "SKU-REL-1")
        .await
        .expect("get stock")
        .expect("stock row exists");
    assert_eq!(after_reserve.available_qty, 7);
    assert_eq!(after_reserve.reserved_qty, 3);

    let reservation_id: Uuid =
        sqlx::query_scalar("select id from reservations where order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("reservation row exists");

    let events_from = consumer
        .latest_offset(INVENTORY_EVENTS_TOPIC)
        .await
        .expect("latest offset before release");

    // Real per-(order_id, "inventory") sequence: version 1 was
    // reserve_inventory, so release_inventory (a second command to the same
    // target) is version 2 -- exactly the bug M06 fixes (see
    // docs/adr/0011-per-target-command-version-counter.md).
    let (_release_event_id, release_bytes) = common::build_release_envelope(
        order_id,
        reservation_id,
        2,
        "payment_failed",
        Uuid::now_v7(),
    );
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        release_bytes,
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
    common::drain_outbox(&pool, &producer).await;
    assert_eq!(summary.applied, 1);

    let after_release = repository::get_stock(&pool, "SKU-REL-1")
        .await
        .expect("get stock")
        .expect("stock row exists");
    assert_eq!(after_release.available_qty, 10, "stock must be restored");
    assert_eq!(after_release.reserved_qty, 0);

    let status: ReservationStatus =
        sqlx::query_scalar("select status from reservations where id = $1")
            .bind(reservation_id)
            .fetch_one(&pool)
            .await
            .expect("reservation status");
    assert_eq!(status, ReservationStatus::Released);

    let released_events: Vec<serde_json::Value> = common::matching_events_for_key(
        &consumer,
        INVENTORY_EVENTS_TOPIC,
        events_from,
        &order_id.to_string(),
        "inventory.inventory_released",
    )
    .await;
    assert_eq!(
        released_events.len(),
        1,
        "exactly one inventory_released event must be published"
    );
}

/// Redelivering the same release command, or a second distinct release
/// command for an already-released reservation, must not double-restore
/// stock or publish a second `inventory_released` event (spec section 12:
/// "Releasing an already released reservation... return[s] logical success
/// without repeating the effect").
#[sqlx::test(migrations = "./migrations")]
async fn duplicate_release_is_idempotent(pool: PgPool) {
    let _topic_guard = common::topic_lock().await;
    let producer = common::connect_producer().await;
    let consumer = common::connect_consumer().await;
    let fault_injector = FaultInjector::new();
    common::seed_offset_to_latest(&pool, &consumer, INVENTORY_COMMANDS_TOPIC).await;

    repository::seed_stock(&pool, "SKU-REL-2", 5, Utc::now())
        .await
        .expect("seed stock");

    let order_id = Uuid::now_v7();
    let (_id, reserve_bytes) =
        common::build_reserve_envelope(order_id, 1, &[("SKU-REL-2", 2)], Uuid::now_v7());
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        reserve_bytes,
    )
    .await;
    common::drain(
        &pool,
        &consumer,
        &producer,
        &fault_injector,
        INVENTORY_COMMANDS_TOPIC,
    )
    .await;
    common::drain_outbox(&pool, &producer).await;
    let reservation_id: Uuid =
        sqlx::query_scalar("select id from reservations where order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("reservation row exists");

    let events_from = consumer
        .latest_offset(INVENTORY_EVENTS_TOPIC)
        .await
        .expect("latest offset before release");

    // Two *distinct* release commands (different event_id each) for the
    // same reservation -- not a raw redelivery, but the reservation-status
    // guard in repository::release must still make the second one a no-op.
    let (_e1, bytes1) = common::build_release_envelope(
        order_id,
        reservation_id,
        2,
        "payment_failed",
        Uuid::now_v7(),
    );
    let (_e2, bytes2) = common::build_release_envelope(
        order_id,
        reservation_id,
        3,
        "payment_failed",
        Uuid::now_v7(),
    );
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes1,
    )
    .await;
    common::publish_raw(
        &producer,
        INVENTORY_COMMANDS_TOPIC,
        &order_id.to_string(),
        bytes2,
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
    common::drain_outbox(&pool, &producer).await;
    assert_eq!(summary.applied, 2, "both distinct commands are processed");

    let stock = repository::get_stock(&pool, "SKU-REL-2")
        .await
        .expect("get stock")
        .expect("stock row exists");
    assert_eq!(stock.available_qty, 5, "stock restored exactly once");
    assert_eq!(stock.reserved_qty, 0);

    let released_events: Vec<serde_json::Value> = common::matching_events_for_key(
        &consumer,
        INVENTORY_EVENTS_TOPIC,
        events_from,
        &order_id.to_string(),
        "inventory.inventory_released",
    )
    .await;
    assert_eq!(
        released_events.len(),
        1,
        "the second release must not publish a second inventory_released event"
    );
}
