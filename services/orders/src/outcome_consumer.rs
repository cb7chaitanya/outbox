//! Consumes downstream outcomes orders reacts to (spec section 12): from
//! `inventory.events.v1` (`reservation_succeeded`, `reservation_failed`,
//! `inventory_released`) and from `payments.events.v1`
//! (`payment_authorized`, `payment_failed`). One module, one consumer name,
//! two topics — `process_available` is called once per topic by two
//! background loops (`main.rs`); dispatch inside `handle_one` is on
//! `event_type`, which is unique across both topics, so nothing here needs
//! to know which topic a record came from beyond what it fetched it from.
//!
//! M06 wires the full choreography-first happy path through
//! `PAYMENT_AUTHORIZED` and the first two rows of the compensation matrix:
//!
//! - `reservation_succeeded` -> `INVENTORY_RESERVED`, records
//!   `reservation_id` (needed later to release it), emits
//!   `authorize_payment`.
//! - `reservation_failed` -> `CANCELLING` -> `CANCELLED` directly (nothing
//!   was ever reserved or paid, so no compensation command is needed —
//!   matrix row 1).
//! - `payment_authorized` -> `PAYMENT_AUTHORIZED`. M06 stops here; driving
//!   fulfilment readiness is M07's job.
//! - `payment_failed` -> `CANCELLING`, emits `release_inventory` (matrix
//!   row 2).
//! - `inventory_released` -> `CANCELLED` (the release confirmation matrix
//!   row 2 waits for before finalizing).
//!
//! Rows 3 and 4 of the compensation matrix (fulfilment failure -> refund +
//! release; compensation retry exhaustion -> `MANUAL_REVIEW`) depend on
//! fulfilment, which doesn't exist until M07 — not implemented here. See
//! `docs/progress.md` for the scope boundary and
//! `docs/adr/0010-orders-consumes-reservation-outcomes.md` /
//! `docs/adr/0011-per-target-command-version-counter.md` for the M05
//! groundwork and version-counter fix this builds on.
//!
//! Structurally this is the same eight-step idempotent-inbox protocol as
//! `inventory::consumer` and `payments::consumer`; see those for the
//! per-step rationale.

use chrono::Utc;
use contracts::Envelope;
use contracts::fulfilment::{
    CREATE_FULFILMENT_AGGREGATE_TYPE, CREATE_FULFILMENT_COMMAND_TYPE,
    CREATE_FULFILMENT_SCHEMA_VERSION, CreateFulfilmentPayload, FULFILMENT_COMMANDS_TOPIC,
    FULFILMENT_CREATED_EVENT_TYPE, FULFILMENT_CREATED_SCHEMA_VERSION, FULFILMENT_FAILED_EVENT_TYPE,
    FULFILMENT_FAILED_SCHEMA_VERSION, FulfilmentCreatedPayload, FulfilmentFailedPayload,
};
use contracts::inventory::{
    INVENTORY_COMMANDS_TOPIC, INVENTORY_RELEASED_EVENT_TYPE, INVENTORY_RELEASED_SCHEMA_VERSION,
    InventoryReleasedPayload, RELEASE_FAILED_EVENT_TYPE, RELEASE_FAILED_SCHEMA_VERSION,
    RELEASE_INVENTORY_AGGREGATE_TYPE, RELEASE_INVENTORY_COMMAND_TYPE,
    RELEASE_INVENTORY_SCHEMA_VERSION, RESERVATION_FAILED_EVENT_TYPE,
    RESERVATION_FAILED_SCHEMA_VERSION, RESERVATION_SUCCEEDED_EVENT_TYPE,
    RESERVATION_SUCCEEDED_SCHEMA_VERSION, ReleaseFailedPayload, ReleaseInventoryPayload,
    ReservationFailedPayload, ReservationSucceededPayload,
};
use contracts::orders::{
    ORDER_AGGREGATE_TYPE, ORDER_CANCELLED_EVENT_TYPE, ORDER_CANCELLED_SCHEMA_VERSION,
    ORDER_COMPLETED_EVENT_TYPE, ORDER_COMPLETED_SCHEMA_VERSION, ORDER_CREATED_TOPIC,
    OrderCancelledPayload, OrderCompletedPayload,
};
use contracts::payments::{
    AUTHORIZE_PAYMENT_COMMAND_TYPE, AUTHORIZE_PAYMENT_SCHEMA_VERSION, AuthorizePaymentPayload,
    PAYMENT_AUTHORIZED_EVENT_TYPE, PAYMENT_AUTHORIZED_SCHEMA_VERSION,
    PAYMENT_COMMAND_AGGREGATE_TYPE, PAYMENT_FAILED_EVENT_TYPE, PAYMENT_FAILED_SCHEMA_VERSION,
    PAYMENT_REFUNDED_EVENT_TYPE, PAYMENT_REFUNDED_SCHEMA_VERSION, PAYMENTS_COMMANDS_TOPIC,
    PaymentAmount, PaymentAuthorizedPayload, PaymentFailedPayload, PaymentRefundedPayload,
    REFUND_FAILED_EVENT_TYPE, REFUND_FAILED_SCHEMA_VERSION, REFUND_PAYMENT_COMMAND_TYPE,
    REFUND_PAYMENT_SCHEMA_VERSION, RefundFailedPayload, RefundPaymentPayload,
};
use messaging::{ConsumedRecord, Consumer, Producer};
use persistence::dlq::DlqRecord;
use persistence::inbox::{NewInboxEntry, VersionDecision};
use persistence::outbox::NewOutboxEvent;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

use crate::domain::OrderStatus;
use crate::repository::{self, TransitionError};

pub const CONSUMER_NAME: &str = "orders-reservation-outcome-consumer";
pub const SOURCE_PARTITION: i32 = 0;

pub const FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT: &str =
    "orders.outcome_consumer.after_db_commit_before_offset_commit";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleOutcome {
    Applied,
    Duplicate,
    Stale,
    Poison,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessSummary {
    pub records_seen: usize,
    pub applied: usize,
    pub duplicate: usize,
    pub stale: usize,
    pub poison: usize,
    pub stopped_by_fault: bool,
}

fn payload_hash(payload: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(payload).expect("json value always serializes");
    hex::encode(Sha256::digest(bytes))
}

async fn publish_dlq(
    producer: &dyn Producer,
    source_topic: &str,
    key: &str,
    record: &ConsumedRecord,
    envelope: Option<serde_json::Value>,
    error_code: &str,
    error_detail: String,
) -> Result<(), messaging::MessagingError> {
    let now = Utc::now();
    let dlq_record = DlqRecord {
        original_topic: source_topic.to_string(),
        original_partition: SOURCE_PARTITION,
        original_offset: record.offset,
        original_key: Some(key.to_string()),
        envelope,
        consumer: CONSUMER_NAME.to_string(),
        attempts: 1,
        first_failure_at: now,
        last_failure_at: now,
        error_code: error_code.to_string(),
        error_detail,
        replay_count: 0,
    };
    persistence::dlq::publish(producer, source_topic, key, &dlq_record).await
}

fn build_authorize_payment_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    command_version: i64,
    currency: &str,
    amount_minor: i64,
) -> NewOutboxEvent {
    let payment_id = Uuid::now_v7();
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: AUTHORIZE_PAYMENT_COMMAND_TYPE.to_string(),
        schema_version: AUTHORIZE_PAYMENT_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: AuthorizePaymentPayload {
            order_id,
            payment_id,
            amount: PaymentAmount {
                currency: currency.to_string(),
                minor_units: amount_minor,
            },
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        topic: PAYMENTS_COMMANDS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

fn build_release_inventory_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    reservation_id: Uuid,
    command_version: i64,
    reason: &str,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: RELEASE_INVENTORY_COMMAND_TYPE.to_string(),
        schema_version: RELEASE_INVENTORY_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: RELEASE_INVENTORY_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: ReleaseInventoryPayload {
            order_id,
            reservation_id,
            reason: reason.to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: RELEASE_INVENTORY_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        topic: INVENTORY_COMMANDS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

fn build_create_fulfilment_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    reservation_id: Uuid,
    payment_id: Uuid,
    command_version: i64,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: CREATE_FULFILMENT_COMMAND_TYPE.to_string(),
        schema_version: CREATE_FULFILMENT_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: CREATE_FULFILMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: CreateFulfilmentPayload {
            order_id,
            reservation_id,
            payment_id,
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: CREATE_FULFILMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        topic: FULFILMENT_COMMANDS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(inner).expect("serializes"),
    }
}

fn build_refund_payment_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    payment_id: Uuid,
    command_version: i64,
    reason: &str,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: REFUND_PAYMENT_COMMAND_TYPE.to_string(),
        schema_version: REFUND_PAYMENT_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".to_string(),
        aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: RefundPaymentPayload {
            order_id,
            payment_id,
            reason: reason.to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: PAYMENT_COMMAND_AGGREGATE_TYPE.to_string(),
        aggregate_id: order_id,
        aggregate_version: command_version,
        topic: PAYMENTS_COMMANDS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(inner).expect("serializes"),
    }
}

fn build_completed_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    fulfilment_id: Uuid,
    order_version: i64,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: ORDER_COMPLETED_EVENT_TYPE.to_string(),
        schema_version: ORDER_COMPLETED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".into(),
        aggregate_type: ORDER_AGGREGATE_TYPE.into(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: OrderCompletedPayload {
            order_id,
            fulfilment_id,
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: ORDER_AGGREGATE_TYPE.into(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        topic: ORDER_CREATED_TOPIC.into(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(inner).expect("serializes"),
    }
}

fn build_cancelled_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    order_version: i64,
    reason: &str,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: ORDER_CANCELLED_EVENT_TYPE.to_string(),
        schema_version: ORDER_CANCELLED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: "orders".into(),
        aggregate_type: ORDER_AGGREGATE_TYPE.into(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: OrderCancelledPayload {
            order_id,
            reason_code: reason.to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: ORDER_AGGREGATE_TYPE.into(),
        aggregate_id: order_id,
        aggregate_version: order_version,
        topic: ORDER_CREATED_TOPIC.into(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(inner).expect("serializes"),
    }
}

/// A redelivery-shaped race (this event reprocessed after the order
/// already advanced past the status this transition requires through some
/// other path) is not this consumer's job to force through, and not a
/// poison message either — the inbox mark below still commits so this
/// exact event id doesn't retry forever.
fn log_illegal_transition_race(order_id: Uuid, from: OrderStatus, to: OrderStatus) {
    tracing::warn!(
        order_id = %order_id,
        %from,
        %to,
        "outcome arrived for an order no longer eligible for this transition"
    );
}

/// Runs the eight-step protocol for one reservation- or payment-outcome
/// record. Never returns `Err` for a business-level problem with the
/// message itself — those are DLQ'd and reported as `Poison`; `Err` is
/// reserved for infrastructure failures that should stop the batch.
async fn handle_one(
    pool: &PgPool,
    producer: &dyn Producer,
    source_topic: &str,
    record: &ConsumedRecord,
) -> Result<HandleOutcome, anyhow::Error> {
    let key = record
        .key
        .as_deref()
        .map(String::from_utf8_lossy)
        .unwrap_or_default()
        .into_owned();

    let envelope: Envelope<serde_json::Value> = match record
        .value
        .as_deref()
        .map(serde_json::from_slice::<Envelope<serde_json::Value>>)
    {
        Some(Ok(env)) => env,
        Some(Err(e)) => {
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                None,
                "MALFORMED_ENVELOPE",
                format!("envelope did not parse as JSON: {e}"),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
        None => {
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                None,
                "MALFORMED_ENVELOPE",
                "record has no value".to_string(),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
    };

    let expected_schema_version = match envelope.event_type.as_str() {
        RESERVATION_SUCCEEDED_EVENT_TYPE => RESERVATION_SUCCEEDED_SCHEMA_VERSION,
        RESERVATION_FAILED_EVENT_TYPE => RESERVATION_FAILED_SCHEMA_VERSION,
        INVENTORY_RELEASED_EVENT_TYPE => INVENTORY_RELEASED_SCHEMA_VERSION,
        RELEASE_FAILED_EVENT_TYPE => RELEASE_FAILED_SCHEMA_VERSION,
        PAYMENT_AUTHORIZED_EVENT_TYPE => PAYMENT_AUTHORIZED_SCHEMA_VERSION,
        PAYMENT_FAILED_EVENT_TYPE => PAYMENT_FAILED_SCHEMA_VERSION,
        PAYMENT_REFUNDED_EVENT_TYPE => PAYMENT_REFUNDED_SCHEMA_VERSION,
        REFUND_FAILED_EVENT_TYPE => REFUND_FAILED_SCHEMA_VERSION,
        FULFILMENT_CREATED_EVENT_TYPE => FULFILMENT_CREATED_SCHEMA_VERSION,
        FULFILMENT_FAILED_EVENT_TYPE => FULFILMENT_FAILED_SCHEMA_VERSION,
        _ => {
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                Some(serde_json::to_value(&envelope).unwrap_or_default()),
                "UNSUPPORTED_SCHEMA",
                format!("unknown event type {}", envelope.event_type),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
    };
    if envelope.schema_version != expected_schema_version {
        publish_dlq(
            producer,
            source_topic,
            &key,
            record,
            Some(serde_json::to_value(&envelope).unwrap_or_default()),
            "UNSUPPORTED_SCHEMA",
            format!(
                "unknown type/version pair {}/{}",
                envelope.event_type, envelope.schema_version
            ),
        )
        .await?;
        return Ok(HandleOutcome::Poison);
    }

    let hash = payload_hash(&envelope.payload);
    let now = Utc::now();

    let mut tx = pool.begin().await?;
    let entry = NewInboxEntry {
        consumer_name: CONSUMER_NAME.to_string(),
        event_id: envelope.event_id,
        source_topic: source_topic.to_string(),
        source_partition: SOURCE_PARTITION,
        source_offset: record.offset,
        aggregate_id: envelope.aggregate_id,
        aggregate_version: envelope.aggregate_version,
        payload_hash: hash.clone(),
    };
    let claimed = persistence::inbox::try_claim(&mut tx, now, &entry).await?;
    if !claimed {
        tx.rollback().await?;
        let existing = persistence::inbox::fetch(pool, CONSUMER_NAME, envelope.event_id)
            .await?
            .expect("try_claim reported a conflict, so a row must exist");
        if existing.payload_hash != hash {
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                Some(serde_json::to_value(&envelope).unwrap_or_default()),
                "INBOX_HASH_MISMATCH",
                "same event id redelivered with a different payload".to_string(),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
        return Ok(HandleOutcome::Duplicate);
    }

    let decision = persistence::inbox::version_decision(
        &mut tx,
        CONSUMER_NAME,
        envelope.aggregate_id,
        envelope.aggregate_version,
    )
    .await?;
    match decision {
        VersionDecision::Stale => {
            persistence::inbox::mark_processed(&mut tx, CONSUMER_NAME, envelope.event_id, now)
                .await?;
            tx.commit().await?;
            return Ok(HandleOutcome::Stale);
        }
        VersionDecision::Gap => {
            persistence::inbox::mark_processed(&mut tx, CONSUMER_NAME, envelope.event_id, now)
                .await?;
            tx.commit().await?;
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                Some(serde_json::to_value(&envelope).unwrap_or_default()),
                "EXPECTED_VERSION_GAP",
                format!(
                    "aggregate {} version {} arrived out of order",
                    envelope.aggregate_id, envelope.aggregate_version
                ),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
        VersionDecision::Apply => {}
    }

    // Everything below runs in the same transaction as the inbox claim
    // above (still open as `tx`) so the inbox mark and the reaction commit
    // or roll back together — a crash between them must not leave the
    // event marked processed with no reaction ever having happened.
    let outcome_result: Result<(), TransitionError> = match envelope.event_type.as_str() {
        RESERVATION_SUCCEEDED_EVENT_TYPE => {
            let payload: ReservationSucceededPayload =
                match serde_json::from_value(envelope.payload.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        tx.rollback().await?;
                        publish_dlq(
                            producer,
                            source_topic,
                            &key,
                            record,
                            Some(serde_json::to_value(&envelope).unwrap_or_default()),
                            "MALFORMED_PAYLOAD",
                            format!("payload did not match reservation_succeeded shape: {e}"),
                        )
                        .await?;
                        return Ok(HandleOutcome::Poison);
                    }
                };
            let order_money = sqlx::query_as::<_, (String, i64)>(
                "select currency, amount_minor from orders where id = $1",
            )
            .bind(payload.order_id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((currency, amount_minor)) = order_money else {
                tx.rollback().await?;
                publish_dlq(
                    producer,
                    source_topic,
                    &key,
                    record,
                    Some(serde_json::to_value(&envelope).unwrap_or_default()),
                    "UNKNOWN_ORDER",
                    format!(
                        "reservation_succeeded for unknown order {}",
                        payload.order_id
                    ),
                )
                .await?;
                return Ok(HandleOutcome::Poison);
            };
            let payments_command_version =
                repository::reserve_command_version(&mut tx, payload.order_id, "payments").await?;
            let envelope_for_event = envelope.clone();
            let reservation_id = payload.reservation_id;
            let order_id = payload.order_id;
            repository::transition_order_with_outbox(
                &mut tx,
                order_id,
                OrderStatus::InventoryReserved,
                Some("inventory reservation succeeded"),
                Some(envelope.event_id),
                Some(reservation_id),
                now,
                move |order_id, _order_version| {
                    vec![build_authorize_payment_event(
                        &envelope_for_event,
                        order_id,
                        payments_command_version,
                        &currency,
                        amount_minor,
                    )]
                },
            )
            .await
            .map(|_| ())
        }
        RESERVATION_FAILED_EVENT_TYPE => {
            let payload: ReservationFailedPayload =
                match serde_json::from_value(envelope.payload.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        tx.rollback().await?;
                        publish_dlq(
                            producer,
                            source_topic,
                            &key,
                            record,
                            Some(serde_json::to_value(&envelope).unwrap_or_default()),
                            "MALFORMED_PAYLOAD",
                            format!("payload did not match reservation_failed shape: {e}"),
                        )
                        .await?;
                        return Ok(HandleOutcome::Poison);
                    }
                };
            // Compensation matrix row 1 (spec section 12): nothing was ever
            // reserved or paid, so cancellation needs no compensation
            // command -- straight to CANCELLING then CANCELLED, in the
            // same transaction.
            let cancelling = repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::Cancelling,
                Some(payload.reason_code.as_str()),
                Some(envelope.event_id),
                None,
                now,
                |_, _| Vec::new(),
            )
            .await;
            match cancelling {
                Ok(_) => repository::transition_order_with_outbox(
                    &mut tx,
                    payload.order_id,
                    OrderStatus::Cancelled,
                    Some("inventory reservation rejected; nothing to compensate"),
                    Some(envelope.event_id),
                    None,
                    now,
                    |_, _| Vec::new(),
                )
                .await
                .map(|_| ()),
                Err(TransitionError::IllegalTransition { from, to }) => {
                    log_illegal_transition_race(payload.order_id, from, to);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        INVENTORY_RELEASED_EVENT_TYPE => {
            let payload: InventoryReleasedPayload =
                match serde_json::from_value(envelope.payload.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        tx.rollback().await?;
                        publish_dlq(
                            producer,
                            source_topic,
                            &key,
                            record,
                            Some(serde_json::to_value(&envelope).unwrap_or_default()),
                            "MALFORMED_PAYLOAD",
                            format!("payload did not match inventory_released shape: {e}"),
                        )
                        .await?;
                        return Ok(HandleOutcome::Poison);
                    }
                };
            // Compensation matrix row 2's second half: the release
            // confirmation is what finally lets the order become
            // CANCELLED (spec section 12: "cancel after release
            // confirmation").
            let waiting_for_refund: Option<bool> = sqlx::query_scalar(
                "update orders set compensation_release_done = true where id = $1 \
                 returning compensation_refund_required and not compensation_refund_done",
            )
            .bind(payload.order_id)
            .fetch_optional(&mut *tx)
            .await?;
            match waiting_for_refund {
                None => Err(TransitionError::NotFound),
                Some(true) => Ok(()),
                Some(false) => {
                    let terminal_envelope = envelope.clone();
                    repository::transition_order_with_outbox(
                        &mut tx,
                        payload.order_id,
                        OrderStatus::Cancelled,
                        Some("required compensations confirmed"),
                        Some(envelope.event_id),
                        None,
                        now,
                        move |order_id, version| {
                            vec![build_cancelled_event(
                                &terminal_envelope,
                                order_id,
                                version,
                                "COMPENSATED",
                            )]
                        },
                    )
                    .await
                    .map(|_| ())
                }
            }
        }
        PAYMENT_AUTHORIZED_EVENT_TYPE => {
            let payload: PaymentAuthorizedPayload =
                match serde_json::from_value(envelope.payload.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        tx.rollback().await?;
                        publish_dlq(
                            producer,
                            source_topic,
                            &key,
                            record,
                            Some(serde_json::to_value(&envelope).unwrap_or_default()),
                            "MALFORMED_PAYLOAD",
                            format!("payload did not match payment_authorized shape: {e}"),
                        )
                        .await?;
                        return Ok(HandleOutcome::Poison);
                    }
                };
            sqlx::query("update orders set payment_id = $1 where id = $2")
                .bind(payload.payment_id)
                .bind(payload.order_id)
                .execute(&mut *tx)
                .await?;
            let payment_transition = repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::PaymentAuthorized,
                Some("payment authorized"),
                Some(envelope.event_id),
                None,
                now,
                |_, _| Vec::new(),
            )
            .await;
            match payment_transition {
                Ok(_) => {}
                Err(TransitionError::IllegalTransition { from, to }) => {
                    log_illegal_transition_race(payload.order_id, from, to);
                    return finish_reaction(tx, &envelope, now).await;
                }
                Err(TransitionError::NotFound) => {
                    tx.rollback().await?;
                    publish_dlq(
                        producer,
                        source_topic,
                        &key,
                        record,
                        Some(serde_json::to_value(&envelope).unwrap_or_default()),
                        "UNKNOWN_ORDER",
                        format!("payment_authorized for unknown order {}", payload.order_id),
                    )
                    .await?;
                    return Ok(HandleOutcome::Poison);
                }
                Err(error) => return Err(error.into()),
            }
            let reservation_id: Uuid =
                sqlx::query_scalar("select reservation_id from orders where id = $1")
                    .bind(payload.order_id)
                    .fetch_one(&mut *tx)
                    .await?;
            let command_version =
                repository::reserve_command_version(&mut tx, payload.order_id, "fulfilment")
                    .await?;
            let envelope_for_event = envelope.clone();
            repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::ReadyForFulfilment,
                Some("inventory and payment confirmed"),
                Some(envelope.event_id),
                None,
                now,
                move |order_id, _| {
                    vec![build_create_fulfilment_event(
                        &envelope_for_event,
                        order_id,
                        reservation_id,
                        payload.payment_id,
                        command_version,
                    )]
                },
            )
            .await
            .map(|_| ())
        }
        PAYMENT_FAILED_EVENT_TYPE => {
            let payload: PaymentFailedPayload =
                match serde_json::from_value(envelope.payload.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        tx.rollback().await?;
                        publish_dlq(
                            producer,
                            source_topic,
                            &key,
                            record,
                            Some(serde_json::to_value(&envelope).unwrap_or_default()),
                            "MALFORMED_PAYLOAD",
                            format!("payload did not match payment_failed shape: {e}"),
                        )
                        .await?;
                        return Ok(HandleOutcome::Poison);
                    }
                };
            let reservation_id: Option<Uuid> =
                sqlx::query_scalar("select reservation_id from orders where id = $1")
                    .bind(payload.order_id)
                    .fetch_optional(&mut *tx)
                    .await?
                    .flatten();
            let Some(reservation_id) = reservation_id else {
                tx.rollback().await?;
                publish_dlq(
                    producer,
                    source_topic,
                    &key,
                    record,
                    Some(serde_json::to_value(&envelope).unwrap_or_default()),
                    "MISSING_RESERVATION_ID",
                    format!(
                        "payment_failed for order {} with no recorded reservation_id",
                        payload.order_id
                    ),
                )
                .await?;
                return Ok(HandleOutcome::Poison);
            };
            let inventory_command_version =
                repository::reserve_command_version(&mut tx, payload.order_id, "inventory").await?;
            let envelope_for_event = envelope.clone();
            let reason_code = payload.reason_code.clone();
            repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::Cancelling,
                Some(payload.reason_code.as_str()),
                Some(envelope.event_id),
                None,
                now,
                move |order_id, _order_version| {
                    vec![build_release_inventory_event(
                        &envelope_for_event,
                        order_id,
                        reservation_id,
                        inventory_command_version,
                        &reason_code,
                    )]
                },
            )
            .await
            .map(|_| ())
        }
        FULFILMENT_CREATED_EVENT_TYPE => {
            let payload: FulfilmentCreatedPayload =
                serde_json::from_value(envelope.payload.clone())?;
            sqlx::query("update orders set fulfilment_id = $1 where id = $2")
                .bind(payload.fulfilment_id)
                .bind(payload.order_id)
                .execute(&mut *tx)
                .await?;
            let terminal_envelope = envelope.clone();
            repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::Completed,
                Some("fulfilment created"),
                Some(envelope.event_id),
                None,
                now,
                move |order_id, version| {
                    vec![build_completed_event(
                        &terminal_envelope,
                        order_id,
                        payload.fulfilment_id,
                        version,
                    )]
                },
            )
            .await
            .map(|_| ())
        }
        FULFILMENT_FAILED_EVENT_TYPE => {
            let payload: FulfilmentFailedPayload =
                serde_json::from_value(envelope.payload.clone())?;
            let facts: Option<(Option<Uuid>, Option<Uuid>)> =
                sqlx::query_as("select reservation_id, payment_id from orders where id = $1")
                    .bind(payload.order_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some((Some(reservation_id), Some(payment_id))) = facts else {
                tx.rollback().await?;
                publish_dlq(
                    producer,
                    source_topic,
                    &key,
                    record,
                    Some(serde_json::to_value(&envelope).unwrap_or_default()),
                    "UNKNOWN_ORDER",
                    format!(
                        "fulfilment_failed for unknown or incomplete order {}",
                        payload.order_id
                    ),
                )
                .await?;
                return Ok(HandleOutcome::Poison);
            };
            sqlx::query(
                "update orders set compensation_release_required = true, \
                compensation_refund_required = true where id = $1",
            )
            .bind(payload.order_id)
            .execute(&mut *tx)
            .await?;
            let inventory_version =
                repository::reserve_command_version(&mut tx, payload.order_id, "inventory").await?;
            let payment_version =
                repository::reserve_command_version(&mut tx, payload.order_id, "payments").await?;
            let envelope_for_event = envelope.clone();
            let reason = payload.reason_code.clone();
            repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::Cancelling,
                Some(&payload.reason_code),
                Some(envelope.event_id),
                None,
                now,
                move |order_id, _| {
                    vec![
                        build_release_inventory_event(
                            &envelope_for_event,
                            order_id,
                            reservation_id,
                            inventory_version,
                            &reason,
                        ),
                        build_refund_payment_event(
                            &envelope_for_event,
                            order_id,
                            payment_id,
                            payment_version,
                            &reason,
                        ),
                    ]
                },
            )
            .await
            .map(|_| ())
        }
        PAYMENT_REFUNDED_EVENT_TYPE => {
            let payload: PaymentRefundedPayload = serde_json::from_value(envelope.payload.clone())?;
            let waiting_for_release: Option<bool> = sqlx::query_scalar(
                "update orders set compensation_refund_done = true where id = $1 \
                 returning compensation_release_required and not compensation_release_done",
            )
            .bind(payload.order_id)
            .fetch_optional(&mut *tx)
            .await?;
            match waiting_for_release {
                None => Err(TransitionError::NotFound),
                Some(true) => Ok(()),
                Some(false) => {
                    let terminal_envelope = envelope.clone();
                    repository::transition_order_with_outbox(
                        &mut tx,
                        payload.order_id,
                        OrderStatus::Cancelled,
                        Some("required compensations confirmed"),
                        Some(envelope.event_id),
                        None,
                        now,
                        move |order_id, version| {
                            vec![build_cancelled_event(
                                &terminal_envelope,
                                order_id,
                                version,
                                "COMPENSATED",
                            )]
                        },
                    )
                    .await
                    .map(|_| ())
                }
            }
        }
        RELEASE_FAILED_EVENT_TYPE => {
            let payload: ReleaseFailedPayload = serde_json::from_value(envelope.payload.clone())?;
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                Some(serde_json::to_value(&envelope).unwrap_or_default()),
                "COMPENSATION_EXHAUSTED",
                payload.reason_code.clone(),
            )
            .await?;
            tracing::error!(order_id = %payload.order_id, reason = %payload.reason_code,
                "compensation exhausted; operator review required");
            repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::ManualReview,
                Some(&payload.reason_code),
                Some(envelope.event_id),
                None,
                now,
                |_, _| Vec::new(),
            )
            .await
            .map(|_| ())
        }
        REFUND_FAILED_EVENT_TYPE => {
            let payload: RefundFailedPayload = serde_json::from_value(envelope.payload.clone())?;
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                Some(serde_json::to_value(&envelope).unwrap_or_default()),
                "COMPENSATION_EXHAUSTED",
                payload.reason_code.clone(),
            )
            .await?;
            tracing::error!(order_id = %payload.order_id, reason = %payload.reason_code,
                "compensation exhausted; operator review required");
            repository::transition_order_with_outbox(
                &mut tx,
                payload.order_id,
                OrderStatus::ManualReview,
                Some(&payload.reason_code),
                Some(envelope.event_id),
                None,
                now,
                |_, _| Vec::new(),
            )
            .await
            .map(|_| ())
        }
        _ => unreachable!("schema dispatch rejected unsupported type"),
    };

    match outcome_result {
        Ok(()) => {}
        Err(TransitionError::IllegalTransition { from, to }) => {
            log_illegal_transition_race(envelope.aggregate_id, from, to);
        }
        Err(TransitionError::NotFound) => {
            // A poison/integrity case (spec section 15), not an
            // infrastructure failure: without this, one record naming an
            // order this consumer's database has no row for would
            // propagate as `Err` out of `process_available` and wedge the
            // offset ledger on that exact record forever (invariant I15 --
            // "a permanently invalid event cannot block its partition
            // forever"), since nothing downstream of a real `Err` here
            // advances past it.
            tx.rollback().await?;
            publish_dlq(
                producer,
                source_topic,
                &key,
                record,
                Some(serde_json::to_value(&envelope).unwrap_or_default()),
                "UNKNOWN_ORDER",
                format!(
                    "{} (aggregate {}) referenced an order this consumer has no row for",
                    envelope.event_type, envelope.aggregate_id
                ),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
        Err(e) => return Err(e.into()),
    }

    persistence::inbox::advance_version(
        &mut tx,
        CONSUMER_NAME,
        envelope.aggregate_id,
        envelope.aggregate_version,
    )
    .await?;
    persistence::inbox::mark_processed(&mut tx, CONSUMER_NAME, envelope.event_id, now).await?;
    tx.commit().await?;

    Ok(HandleOutcome::Applied)
}

async fn finish_reaction(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    envelope: &Envelope<serde_json::Value>,
    now: chrono::DateTime<Utc>,
) -> Result<HandleOutcome, anyhow::Error> {
    persistence::inbox::advance_version(
        &mut tx,
        CONSUMER_NAME,
        envelope.aggregate_id,
        envelope.aggregate_version,
    )
    .await?;
    persistence::inbox::mark_processed(&mut tx, CONSUMER_NAME, envelope.event_id, now).await?;
    tx.commit().await?;
    Ok(HandleOutcome::Applied)
}

/// Fetches whatever is new on `source_topic` since the last committed
/// offset and runs [`handle_one`] on each record in order, advancing the
/// offset ledger after each one — unless the fault point fires, which
/// stops the batch immediately without advancing past that record.
pub async fn process_available(
    pool: &PgPool,
    consumer: &dyn Consumer,
    producer: &dyn Producer,
    fault_injector: &FaultInjector,
    source_topic: &str,
    max_wait_ms: i32,
) -> Result<ProcessSummary, anyhow::Error> {
    let start_offset =
        persistence::inbox::fetch_offset(pool, CONSUMER_NAME, source_topic, SOURCE_PARTITION)
            .await?;
    let records = consumer
        .fetch(source_topic, start_offset, max_wait_ms)
        .await?;

    let mut summary = ProcessSummary {
        records_seen: records.len(),
        ..Default::default()
    };

    let ordered_records = messaging::order_by_aggregate_version(&records);
    let mut offset_tracker = messaging::ContiguousOffsetTracker::new(start_offset);
    for record in &ordered_records {
        let outcome = handle_one(pool, producer, source_topic, record).await?;
        match outcome {
            HandleOutcome::Applied => summary.applied += 1,
            HandleOutcome::Duplicate => summary.duplicate += 1,
            HandleOutcome::Stale => summary.stale += 1,
            HandleOutcome::Poison => summary.poison += 1,
        }

        if fault_injector
            .maybe_fail(
                FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT,
                Some(&record.offset.to_string()),
            )
            .await
            .is_err()
        {
            summary.stopped_by_fault = true;
            return Ok(summary);
        }

        if let Some(next_offset) = offset_tracker.complete(record.offset) {
            persistence::inbox::commit_offset(
                pool,
                CONSUMER_NAME,
                source_topic,
                SOURCE_PARTITION,
                next_offset,
                Utc::now(),
            )
            .await?;
        }
    }

    Ok(summary)
}
