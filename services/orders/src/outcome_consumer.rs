//! Consumes reservation outcomes from `inventory.events.v1` (spec section
//! 12: "Payment authorization begins only after reservation succeeds") and
//! reacts just enough to make the payments service (M05) exercisable
//! end-to-end: on `reservation_succeeded`, transition the order to
//! `INVENTORY_RESERVED` and emit `payments.authorize_payment` through the
//! outbox, in one transaction ([`crate::repository::transition_order_with_outbox`]).
//!
//! `reservation_failed` is intentionally a no-op beyond marking the inbox
//! record processed: driving the order to `CANCELLING`/`CANCELLED` is the
//! compensation matrix's job (spec section 12), which M06 implements in
//! full. Wiring only the success half here — rather than a half-finished
//! cancellation path — keeps this milestone's scope to "prove payments
//! works end-to-end," documented in
//! `docs/adr/0010-orders-consumes-reservation-outcomes.md`.
//!
//! Structurally this is the same eight-step idempotent-inbox protocol as
//! `inventory::consumer` and `payments::consumer`; see those for the
//! per-step rationale.

use chrono::Utc;
use contracts::Envelope;
use contracts::inventory::{
    RESERVATION_FAILED_EVENT_TYPE, RESERVATION_FAILED_SCHEMA_VERSION,
    RESERVATION_SUCCEEDED_EVENT_TYPE, RESERVATION_SUCCEEDED_SCHEMA_VERSION,
    ReservationFailedPayload, ReservationSucceededPayload,
};
use contracts::payments::{
    AUTHORIZE_PAYMENT_COMMAND_TYPE, AUTHORIZE_PAYMENT_SCHEMA_VERSION, AuthorizePaymentPayload,
    PAYMENT_COMMAND_AGGREGATE_TYPE, PAYMENTS_COMMANDS_TOPIC, PaymentAmount,
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

/// `aggregate_version` here is deliberately **not** the order's own
/// version (which is 2 by the time this command is built, since the
/// reservation-success transition already happened) — it's this
/// command's position in the orders→payments command stream specifically,
/// which the payments consumer tracks independently via its own
/// `(consumer_name, aggregate_id)` row (spec section 14). Using the
/// order's global version here would make payments see a gap on its very
/// first message for this order (last_version 0, incoming 2), since
/// payments never receives a message at order-version 1 — that one went
/// to inventory instead. `1` is correct because this is, in this
/// milestone's scope, the only command orders ever sends to payments for
/// a given order; M06 (refunds are the second such command) must turn
/// this into a real per-order, per-downstream-consumer counter rather
/// than a constant. See
/// `docs/adr/0010-orders-consumes-reservation-outcomes.md`.
const FIRST_PAYMENTS_COMMAND_VERSION: i64 = 1;

fn build_authorize_payment_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
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
        aggregate_version: FIRST_PAYMENTS_COMMAND_VERSION,
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
        aggregate_version: FIRST_PAYMENTS_COMMAND_VERSION,
        topic: PAYMENTS_COMMANDS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

/// Runs the eight-step protocol for one reservation-outcome record. Never
/// returns `Err` for a business-level problem with the message itself —
/// those are DLQ'd and reported as `Poison`; `Err` is reserved for
/// infrastructure failures that should stop the batch.
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
    if envelope.event_type == RESERVATION_SUCCEEDED_EVENT_TYPE {
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
        let envelope_for_event = envelope.clone();
        let result = repository::transition_order_with_outbox(
            &mut tx,
            payload.order_id,
            OrderStatus::InventoryReserved,
            Some("inventory reservation succeeded"),
            Some(envelope.event_id),
            now,
            move |order_id, _order_version| {
                vec![build_authorize_payment_event(
                    &envelope_for_event,
                    order_id,
                    &currency,
                    amount_minor,
                )]
            },
        )
        .await;
        match result {
            Ok(_) => {}
            Err(TransitionError::IllegalTransition { from, to }) => {
                // Redelivery-shaped race (e.g. this event reprocessed after
                // the order already advanced past PENDING through some
                // other path): not this consumer's job to force it, and
                // not a poison message either — still commits the inbox
                // mark below so this exact event id doesn't retry forever.
                tracing::warn!(order_id = %payload.order_id, %from, %to, "reservation_succeeded arrived for an order no longer eligible for this transition");
            }
            Err(e) => return Err(e.into()),
        }
    } else {
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
        // M06 scope (spec section 12's compensation matrix): drive the
        // order to CANCELLING/CANCELLED here. For M05, recording that this
        // consumer saw the failure (via the inbox mark below) is enough to
        // keep it from blocking; see docs/adr/0010.
        tracing::info!(order_id = %payload.order_id, reason_code = %payload.reason_code, "reservation failed; compensation wiring lands in M06");
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

    for record in &records {
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

        persistence::inbox::commit_offset(
            pool,
            CONSUMER_NAME,
            source_topic,
            SOURCE_PARTITION,
            record.offset + 1,
            Utc::now(),
        )
        .await?;
    }

    Ok(summary)
}
