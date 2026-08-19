//! Repository-layer tests against a real Postgres database (spec section
//! 18). Each test gets a freshly migrated, isolated database via
//! `#[sqlx::test]`.

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use orders::domain::{CreateOrderRequest, ItemRequest, OrderStatus, validate_and_normalize};
use orders::repository::{self, TransitionError};

fn sample_request() -> CreateOrderRequest {
    CreateOrderRequest {
        items: vec![
            ItemRequest {
                sku: "SKU-1".to_string(),
                quantity: 2,
                unit_price_minor: 1250,
            },
            ItemRequest {
                sku: "SKU-2".to_string(),
                quantity: 1,
                unit_price_minor: 500,
            },
        ],
        currency: "USD".to_string(),
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_identical_idempotent_requests_yield_one_order(pool: PgPool) {
    let normalized = validate_and_normalize(sample_request()).unwrap();
    let key = "concurrent-key-001";

    let mut handles = Vec::new();
    for _ in 0..10 {
        let pool = pool.clone();
        let normalized = normalized.clone();
        handles.push(tokio::spawn(async move {
            repository::create_order(
                &pool,
                key,
                &normalized,
                Uuid::now_v7(),
                Utc::now(),
                |_, _| None,
            )
            .await
        }));
    }

    let mut order_ids = std::collections::HashSet::new();
    for handle in handles {
        let outcome = handle
            .await
            .unwrap()
            .expect("create_order must not fail under contention");
        order_ids.insert(outcome.order.id);
    }

    assert_eq!(
        order_ids.len(),
        1,
        "all concurrent callers must observe the same order id"
    );

    let row_count: i64 = sqlx::query_scalar("select count(*) from orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row_count, 1, "exactly one order row must exist");

    let item_count: i64 = sqlx::query_scalar("select count(*) from order_items")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(item_count, 2, "items must not be duplicated across retries");
}

#[sqlx::test(migrations = "./migrations")]
async fn reused_idempotency_key_with_different_body_is_rejected(pool: PgPool) {
    let key = "reuse-key-001";
    let first = validate_and_normalize(sample_request()).unwrap();
    repository::create_order(&pool, key, &first, Uuid::now_v7(), Utc::now(), |_, _| None)
        .await
        .unwrap();

    let mut different = sample_request();
    different.items.push(ItemRequest {
        sku: "SKU-3".to_string(),
        quantity: 1,
        unit_price_minor: 100,
    });
    let different = validate_and_normalize(different).unwrap();

    let result = repository::create_order(
        &pool,
        key,
        &different,
        Uuid::now_v7(),
        Utc::now(),
        |_, _| None,
    )
    .await;
    assert!(
        matches!(result, Err(repository::RepoError::IdempotencyKeyReused)),
        "expected IdempotencyKeyReused, got {result:?}"
    );

    let row_count: i64 = sqlx::query_scalar("select count(*) from orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        row_count, 1,
        "the rejected request must not create a second order"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn same_key_same_body_replays_original_order(pool: PgPool) {
    let key = "replay-key-001";
    let normalized = validate_and_normalize(sample_request()).unwrap();

    let first = repository::create_order(
        &pool,
        key,
        &normalized,
        Uuid::now_v7(),
        Utc::now(),
        |_, _| None,
    )
    .await
    .unwrap();
    let second = repository::create_order(
        &pool,
        key,
        &normalized,
        Uuid::now_v7(),
        Utc::now(),
        |_, _| None,
    )
    .await
    .unwrap();

    assert_eq!(first.order.id, second.order.id);
    assert!(first.created);
    assert!(
        !second.created,
        "second call must be a replay, not a fresh insert"
    );
}

async fn create_pending_order(pool: &PgPool, key: &str) -> repository::OrderRow {
    let normalized = validate_and_normalize(sample_request()).unwrap();
    repository::create_order(
        pool,
        key,
        &normalized,
        Uuid::now_v7(),
        Utc::now(),
        |_, _| None,
    )
    .await
    .unwrap()
    .order
}

#[sqlx::test(migrations = "./migrations")]
async fn illegal_transition_is_rejected_without_partial_write(pool: PgPool) {
    let order = create_pending_order(&pool, "illegal-key-001").await;
    assert_eq!(order.status, OrderStatus::Pending);
    assert_eq!(order.version, 1);

    // PENDING -> COMPLETED skips the entire required graph and is illegal.
    let result = repository::transition_order(
        &pool,
        order.id,
        order.version,
        OrderStatus::Completed,
        Some("attempted illegal jump"),
        Utc::now(),
    )
    .await;

    assert!(
        matches!(
            result,
            Err(TransitionError::IllegalTransition {
                from: OrderStatus::Pending,
                to: OrderStatus::Completed
            })
        ),
        "expected IllegalTransition, got {result:?}"
    );

    let (reloaded, _) = repository::get_order(&pool, order.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reloaded.status,
        OrderStatus::Pending,
        "status must be unchanged"
    );
    assert_eq!(reloaded.version, 1, "version must be unchanged");

    let transitions = repository::get_transitions(&pool, order.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        transitions.len(),
        1,
        "no new transition row must be inserted"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn stale_expected_version_is_rejected_without_partial_write(pool: PgPool) {
    let order = create_pending_order(&pool, "stale-key-001").await;
    assert_eq!(order.version, 1);

    // The transition itself (PENDING -> INVENTORY_RESERVED) is legal; only
    // the expected version is wrong, simulating a caller acting on stale
    // knowledge of the aggregate.
    let stale_expected_version = order.version + 41;
    let result = repository::transition_order(
        &pool,
        order.id,
        stale_expected_version,
        OrderStatus::InventoryReserved,
        Some("stale caller"),
        Utc::now(),
    )
    .await;

    assert!(
        matches!(
            result,
            Err(TransitionError::VersionConflict { expected }) if expected == stale_expected_version
        ),
        "expected VersionConflict, got {result:?}"
    );

    let (reloaded, _) = repository::get_order(&pool, order.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reloaded.status,
        OrderStatus::Pending,
        "status must be unchanged"
    );
    assert_eq!(reloaded.version, 1, "version must be unchanged");

    let transitions = repository::get_transitions(&pool, order.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        transitions.len(),
        1,
        "no new transition row must be inserted"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn legal_transition_with_correct_version_succeeds_and_is_recorded(pool: PgPool) {
    let order = create_pending_order(&pool, "legal-key-001").await;

    let updated = repository::transition_order(
        &pool,
        order.id,
        order.version,
        OrderStatus::InventoryReserved,
        Some("inventory reserved"),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(updated.status, OrderStatus::InventoryReserved);
    assert_eq!(updated.version, 2);

    let transitions = repository::get_transitions(&pool, order.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(transitions.len(), 2);
    assert_eq!(transitions[1].from_status, Some(OrderStatus::Pending));
    assert_eq!(transitions[1].to_status, OrderStatus::InventoryReserved);
    assert_eq!(transitions[1].order_version, 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn transition_on_missing_order_is_not_found(pool: PgPool) {
    let result = repository::transition_order(
        &pool,
        Uuid::now_v7(),
        1,
        OrderStatus::InventoryReserved,
        None,
        Utc::now(),
    )
    .await;
    assert!(matches!(result, Err(TransitionError::NotFound)));
}
