//! The `inventory.reserve_inventory` consumer: the idempotent-inbox
//! handler protocol from spec section 14, applied to reservation requests.
//!
//! [`process_available`] is the unit of work a background loop (or a test)
//! calls repeatedly: fetch whatever is new since the last committed
//! offset, and for each record run the eight-step protocol — validate,
//! claim inbox identity, check the payload hash on a duplicate, decide
//! apply/stale/gap by aggregate version, apply the business mutation with
//! its resulting outbox event in one transaction, mark the inbox row
//! processed, commit, and only then advance this project's local offset
//! ledger (see `docs/adr/0006-inbox-consumer-offset-ledger.md`).

use chrono::Utc;
use contracts::Envelope;
use contracts::inventory::{
    INVENTORY_PRODUCER_NAME, INVENTORY_RELEASED_EVENT_TYPE, INVENTORY_RELEASED_SCHEMA_VERSION,
    InventoryReleasedPayload, RELEASE_INVENTORY_COMMAND_TYPE, RELEASE_INVENTORY_SCHEMA_VERSION,
    RESERVATION_AGGREGATE_TYPE, RESERVATION_FAILED_EVENT_TYPE, RESERVATION_FAILED_SCHEMA_VERSION,
    RESERVATION_SUCCEEDED_EVENT_TYPE, RESERVATION_SUCCEEDED_SCHEMA_VERSION,
    RESERVE_INVENTORY_COMMAND_TYPE, RESERVE_INVENTORY_SCHEMA_VERSION, ReleaseInventoryPayload,
    ReservationFailedPayload, ReservationSucceededPayload, ReserveInventoryItem,
    ReserveInventoryPayload,
};
use messaging::{ConsumedRecord, Consumer, Producer};
use persistence::dlq::DlqRecord;
use persistence::inbox::{NewInboxEntry, VersionDecision};
use persistence::outbox::NewOutboxEvent;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

use crate::domain::{self, ReservationStatus};

pub const CONSUMER_NAME: &str = "inventory-reserve-consumer";
pub const SOURCE_PARTITION: i32 = 0;

/// Fault point (spec section 17: "after consumer DB commit/before offset
/// commit"). Fires once per record, after that record's business
/// transaction (or DLQ publish) has already committed, and before this
/// service's offset ledger is advanced past it — simulating a process
/// crash in exactly that window. The record is then redelivered on the
/// next fetch; the inbox row already committed for it makes reprocessing
/// a safe no-op (spec section 14: "redelivery becomes a no-op because of
/// the inbox row").
pub const FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT: &str =
    "inventory.after_db_commit_before_offset_commit";

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
    /// `true` if the fault point fired and processing stopped before
    /// advancing the offset past the last handled record.
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

/// Runs the eight-step protocol for one record. Never returns `Err` for a
/// business-level problem with the message itself (malformed envelope,
/// unsupported schema, hash mismatch) — those are DLQ'd and reported as
/// `Poison`; `Err` is reserved for infrastructure failures (DB/broker)
/// that should stop the batch rather than be treated as handled.
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

    // Step 1: validate envelope/payload.
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
        RESERVE_INVENTORY_COMMAND_TYPE => RESERVE_INVENTORY_SCHEMA_VERSION,
        RELEASE_INVENTORY_COMMAND_TYPE => RELEASE_INVENTORY_SCHEMA_VERSION,
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

    // Steps 2-3: claim inbox identity, or verify a duplicate's hash.
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

    // Step 4: ordering/replay policy.
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
            persistence::metrics::record_gap();
            // M04 scope boundary (documented in ADR 0008): a bounded
            // retry/buffer window before DLQ is M08's ordering-hardening
            // milestone. For now an out-of-order arrival goes straight to
            // DLQ with the spec's `EXPECTED_VERSION_GAP` code, and the
            // inbox claim above is kept (commit, don't rollback) so a
            // redelivery of this exact event id short-circuits as a
            // duplicate rather than re-triggering a second DLQ record.
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

    // Step 5: apply the business mutation, dispatching on event_type since
    // both commands share `inventory.commands.v1`.
    let outbox_events: Vec<NewOutboxEvent> = if envelope.event_type
        == RESERVE_INVENTORY_COMMAND_TYPE
    {
        let payload: ReserveInventoryPayload =
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
                        format!("payload did not match reserve_inventory shape: {e}"),
                    )
                    .await?;
                    return Ok(HandleOutcome::Poison);
                }
            };
        let raw_items: Vec<(String, i64)> = payload
            .items
            .iter()
            .map(|i| (i.sku.clone(), i.quantity))
            .collect();
        let lines = match domain::validate_items(&raw_items) {
            Ok(lines) => lines,
            Err(e) => {
                tx.rollback().await?;
                publish_dlq(
                    producer,
                    source_topic,
                    &key,
                    record,
                    Some(serde_json::to_value(&envelope).unwrap_or_default()),
                    "INVALID_RESERVATION_REQUEST",
                    e.to_string(),
                )
                .await?;
                return Ok(HandleOutcome::Poison);
            }
        };
        let outcome = crate::repository::reserve(&mut tx, payload.order_id, &lines, now).await?;
        vec![build_reservation_outcome_event(
            &envelope, &payload, &outcome,
        )]
    } else {
        let payload: ReleaseInventoryPayload =
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
                        format!("payload did not match release_inventory shape: {e}"),
                    )
                    .await?;
                    return Ok(HandleOutcome::Poison);
                }
            };
        let outcome = crate::repository::release(&mut tx, payload.order_id, now).await?;
        if outcome.created {
            vec![build_released_event(
                &envelope,
                &payload,
                outcome.reservation_id,
            )]
        } else {
            Vec::new()
        }
    };

    // Step 6: insert the resulting outbox event(s), in the same transaction.
    for outbox_event in outbox_events {
        persistence::outbox::insert(&mut tx, now, &outbox_event).await?;
    }

    persistence::inbox::advance_version(
        &mut tx,
        CONSUMER_NAME,
        envelope.aggregate_id,
        envelope.aggregate_version,
    )
    .await?;

    // Step 7: mark inbox processed and commit.
    persistence::inbox::mark_processed(&mut tx, CONSUMER_NAME, envelope.event_id, now).await?;
    tx.commit().await?;

    tracing::info!(event_id = %envelope.event_id, correlation_id = %envelope.correlation_id,
        causation_id = %envelope.causation_id, aggregate_id = %envelope.aggregate_id,
        aggregate_version = envelope.aggregate_version, topic = source_topic,
        partition = SOURCE_PARTITION, offset = record.offset, result = "applied", "event handled");

    Ok(HandleOutcome::Applied)
}

fn build_reservation_outcome_event(
    envelope: &Envelope<serde_json::Value>,
    payload: &ReserveInventoryPayload,
    outcome: &crate::repository::ReserveOutcome,
) -> NewOutboxEvent {
    let items: Vec<ReserveInventoryItem> = payload
        .items
        .iter()
        .map(|i| ReserveInventoryItem {
            sku: i.sku.clone(),
            quantity: i.quantity,
        })
        .collect();
    let event_id = Uuid::now_v7();
    let now = Utc::now();

    match outcome.status {
        ReservationStatus::Active => {
            let succeeded = Envelope {
                event_id,
                event_type: RESERVATION_SUCCEEDED_EVENT_TYPE.to_string(),
                schema_version: RESERVATION_SUCCEEDED_SCHEMA_VERSION,
                occurred_at: now,
                producer: INVENTORY_PRODUCER_NAME.to_string(),
                aggregate_type: RESERVATION_AGGREGATE_TYPE.to_string(),
                aggregate_id: outcome.reservation_id,
                aggregate_version: 1,
                correlation_id: envelope.correlation_id,
                causation_id: envelope.event_id,
                traceparent: envelope.traceparent.clone(),
                payload: ReservationSucceededPayload {
                    order_id: payload.order_id,
                    reservation_id: outcome.reservation_id,
                    items,
                },
            };
            NewOutboxEvent {
                id: event_id,
                aggregate_type: RESERVATION_AGGREGATE_TYPE.to_string(),
                aggregate_id: outcome.reservation_id,
                aggregate_version: 1,
                topic: contracts::inventory::INVENTORY_EVENTS_TOPIC.to_string(),
                message_key: payload.order_id.to_string(),
                envelope: serde_json::to_value(&succeeded).expect("envelope serializes"),
            }
        }
        _ => {
            let failed = Envelope {
                event_id,
                event_type: RESERVATION_FAILED_EVENT_TYPE.to_string(),
                schema_version: RESERVATION_FAILED_SCHEMA_VERSION,
                occurred_at: now,
                producer: INVENTORY_PRODUCER_NAME.to_string(),
                aggregate_type: RESERVATION_AGGREGATE_TYPE.to_string(),
                aggregate_id: outcome.reservation_id,
                aggregate_version: 1,
                correlation_id: envelope.correlation_id,
                causation_id: envelope.event_id,
                traceparent: envelope.traceparent.clone(),
                payload: ReservationFailedPayload {
                    order_id: payload.order_id,
                    reason_code: outcome
                        .reason_code
                        .clone()
                        .unwrap_or_else(|| "UNKNOWN".to_string()),
                    items,
                },
            };
            NewOutboxEvent {
                id: event_id,
                aggregate_type: RESERVATION_AGGREGATE_TYPE.to_string(),
                aggregate_id: outcome.reservation_id,
                aggregate_version: 1,
                topic: contracts::inventory::INVENTORY_EVENTS_TOPIC.to_string(),
                message_key: payload.order_id.to_string(),
                envelope: serde_json::to_value(&failed).expect("envelope serializes"),
            }
        }
    }
}

/// `aggregate_version: 2` — the reservation aggregate's outbox-emitting
/// lifecycle is fixed and known ahead of time (unlike orders' open-ended
/// outbound command relationships): version 1 is always
/// `reservation_succeeded`/`reservation_failed`, and `inventory_released`
/// (only ever emitted for a reservation that *was* active, per
/// [`crate::repository::release`]'s `created` guard) is always the second
/// and last event on that same aggregate.
fn build_released_event(
    envelope: &Envelope<serde_json::Value>,
    payload: &ReleaseInventoryPayload,
    reservation_id: Uuid,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: INVENTORY_RELEASED_EVENT_TYPE.to_string(),
        schema_version: INVENTORY_RELEASED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: INVENTORY_PRODUCER_NAME.to_string(),
        aggregate_type: RESERVATION_AGGREGATE_TYPE.to_string(),
        aggregate_id: reservation_id,
        aggregate_version: 2,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: envelope.traceparent.clone(),
        payload: InventoryReleasedPayload {
            order_id: payload.order_id,
            reservation_id,
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: RESERVATION_AGGREGATE_TYPE.to_string(),
        aggregate_id: reservation_id,
        aggregate_version: 2,
        topic: contracts::inventory::INVENTORY_EVENTS_TOPIC.to_string(),
        message_key: payload.order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
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
    persistence::metrics::set_consumer_lag(
        consumer.latest_offset(source_topic).await? - start_offset,
    );
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
            HandleOutcome::Applied => {
                summary.applied += 1;
                persistence::metrics::record_result("applied");
            }
            HandleOutcome::Duplicate => {
                summary.duplicate += 1;
                persistence::metrics::record_result("duplicate");
            }
            HandleOutcome::Stale => {
                summary.stale += 1;
                persistence::metrics::record_result("stale");
            }
            HandleOutcome::Poison => {
                summary.poison += 1;
                persistence::metrics::record_result("poison");
            }
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
