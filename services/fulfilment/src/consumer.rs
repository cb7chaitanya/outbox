//! The `fulfilment.commands.v1` consumer: the idempotent-inbox handler
//! protocol from spec section 14, applied to `create_fulfilment`.
//!
//! Structurally this is the same eight-step protocol as
//! `inventory::consumer` / `payments::consumer`; see those for the
//! per-step rationale. Two fault points (spec section 17: "fulfilment
//! permanent/transient failure") drive the two ways `create_fulfilment`
//! can fail: [`FAULT_PERMANENT_FAILURE`] fails immediately with no retry
//! (a business rejection, e.g. a carrier permanently unable to take the
//! shipment); [`FAULT_TRANSIENT_FAILURE`] is retried with the same
//! full-jitter backoff budget as payments' provider calls (spec section
//! 15 defaults) and only becomes a failure once that budget is exhausted.
//! Either way the outcome is the same event type,
//! `fulfilment.fulfilment_failed` — orders' compensation matrix row 3
//! (refund + release) reacts identically regardless of *why* fulfilment
//! failed.

use std::time::{Duration as StdDuration, Instant};

use chrono::Utc;
use contracts::Envelope;
use contracts::fulfilment::{
    CREATE_FULFILMENT_COMMAND_TYPE, CREATE_FULFILMENT_SCHEMA_VERSION, CreateFulfilmentPayload,
    FULFILMENT_AGGREGATE_TYPE, FULFILMENT_CREATED_EVENT_TYPE, FULFILMENT_CREATED_SCHEMA_VERSION,
    FULFILMENT_EVENTS_TOPIC, FULFILMENT_FAILED_EVENT_TYPE, FULFILMENT_FAILED_SCHEMA_VERSION,
    FULFILMENT_PRODUCER_NAME, FulfilmentCreatedPayload, FulfilmentFailedPayload,
};
use messaging::{ConsumedRecord, Consumer, Producer};
use persistence::dlq::DlqRecord;
use persistence::inbox::{NewInboxEntry, VersionDecision};
use persistence::outbox::NewOutboxEvent;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use test_support::FaultInjector;
use uuid::Uuid;

use crate::domain::FulfilmentStatus;
use crate::repository;

pub const CONSUMER_NAME: &str = "fulfilment-consumer";
pub const SOURCE_PARTITION: i32 = 0;

pub const FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT: &str =
    "fulfilment.after_db_commit_before_offset_commit";
pub const FAULT_PERMANENT_FAILURE: &str = "fulfilment.permanent_failure";
pub const FAULT_TRANSIENT_FAILURE: &str = "fulfilment.transient_failure";

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub max_elapsed: StdDuration,
    pub backoff_base: StdDuration,
    pub backoff_cap: StdDuration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            max_elapsed: StdDuration::from_secs(600),
            backoff_base: StdDuration::from_millis(100),
            backoff_cap: StdDuration::from_secs(30),
        }
    }
}

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

fn build_created_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    fulfilment_id: Uuid,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: FULFILMENT_CREATED_EVENT_TYPE.to_string(),
        schema_version: FULFILMENT_CREATED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: FULFILMENT_PRODUCER_NAME.to_string(),
        aggregate_type: FULFILMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: fulfilment_id,
        aggregate_version: 1,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: FulfilmentCreatedPayload {
            order_id,
            fulfilment_id,
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: FULFILMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: fulfilment_id,
        aggregate_version: 1,
        topic: FULFILMENT_EVENTS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

fn build_failed_event(
    envelope: &Envelope<serde_json::Value>,
    order_id: Uuid,
    fulfilment_id: Uuid,
    reason_code: &str,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: FULFILMENT_FAILED_EVENT_TYPE.to_string(),
        schema_version: FULFILMENT_FAILED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: FULFILMENT_PRODUCER_NAME.to_string(),
        aggregate_type: FULFILMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: fulfilment_id,
        aggregate_version: 1,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: FulfilmentFailedPayload {
            order_id,
            fulfilment_id,
            reason_code: reason_code.to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: FULFILMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: fulfilment_id,
        aggregate_version: 1,
        topic: FULFILMENT_EVENTS_TOPIC.to_string(),
        message_key: order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

/// Runs the eight-step protocol for one `create_fulfilment` record. Never
/// returns `Err` for a business-level problem with the message itself —
/// those are DLQ'd and reported as `Poison`; `Err` is reserved for
/// infrastructure failures that should stop the batch.
async fn handle_one(
    pool: &PgPool,
    producer: &dyn Producer,
    fault_injector: &FaultInjector,
    source_topic: &str,
    record: &ConsumedRecord,
    retry_config: &RetryConfig,
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

    if envelope.event_type != CREATE_FULFILMENT_COMMAND_TYPE
        || envelope.schema_version != CREATE_FULFILMENT_SCHEMA_VERSION
    {
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

    let payload: CreateFulfilmentPayload = match serde_json::from_value(envelope.payload.clone()) {
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
                format!("payload did not match create_fulfilment shape: {e}"),
            )
            .await?;
            return Ok(HandleOutcome::Poison);
        }
    };

    let outbox_event = if fault_injector
        .maybe_fail(FAULT_PERMANENT_FAILURE, Some(&payload.order_id.to_string()))
        .await
        .is_err()
    {
        let outcome = repository::record_outcome(
            &mut tx,
            payload.order_id,
            payload.reservation_id,
            payload.payment_id,
            FulfilmentStatus::Failed,
            Some("FULFILMENT_REJECTED"),
            now,
        )
        .await?;
        build_failed_event(
            &envelope,
            payload.order_id,
            outcome.fulfilment_id,
            outcome
                .failure_code
                .as_deref()
                .unwrap_or("FULFILMENT_REJECTED"),
        )
    } else {
        let start = Instant::now();
        let mut attempts_made: u32 = 0;
        let mut succeeded = false;
        while attempts_made < retry_config.max_attempts {
            attempts_made += 1;
            if fault_injector
                .maybe_fail(FAULT_TRANSIENT_FAILURE, Some(&payload.order_id.to_string()))
                .await
                .is_ok()
            {
                succeeded = true;
                break;
            }
            if attempts_made >= retry_config.max_attempts
                || start.elapsed() >= retry_config.max_elapsed
            {
                break;
            }
            let delay = persistence::outbox::full_jitter_backoff(
                attempts_made - 1,
                retry_config.backoff_base,
                retry_config.backoff_cap,
            );
            tokio::time::sleep(delay).await;
        }

        if succeeded {
            let outcome = repository::record_outcome(
                &mut tx,
                payload.order_id,
                payload.reservation_id,
                payload.payment_id,
                FulfilmentStatus::Created,
                None,
                now,
            )
            .await?;
            build_created_event(&envelope, payload.order_id, outcome.fulfilment_id)
        } else {
            let outcome = repository::record_outcome(
                &mut tx,
                payload.order_id,
                payload.reservation_id,
                payload.payment_id,
                FulfilmentStatus::Failed,
                Some("FULFILMENT_RETRY_BUDGET_EXHAUSTED"),
                now,
            )
            .await?;
            build_failed_event(
                &envelope,
                payload.order_id,
                outcome.fulfilment_id,
                outcome
                    .failure_code
                    .as_deref()
                    .unwrap_or("FULFILMENT_RETRY_BUDGET_EXHAUSTED"),
            )
        }
    };

    persistence::outbox::insert(&mut tx, now, &outbox_event).await?;

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
#[allow(clippy::too_many_arguments)]
pub async fn process_available(
    pool: &PgPool,
    consumer: &dyn Consumer,
    producer: &dyn Producer,
    fault_injector: &FaultInjector,
    source_topic: &str,
    max_wait_ms: i32,
    retry_config: &RetryConfig,
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
        let outcome = handle_one(
            pool,
            producer,
            fault_injector,
            source_topic,
            record,
            retry_config,
        )
        .await?;
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
