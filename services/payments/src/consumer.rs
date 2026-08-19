//! The `payments.commands.v1` consumer: the idempotent-inbox handler
//! protocol from spec section 14, applied to both `authorize_payment` and
//! `refund_payment` (they share one topic; dispatch is on `event_type`).
//!
//! Three layers of idempotency stack here, each covering a different
//! redelivery/retry scenario: the inbox's `(consumer_name, event_id)`
//! dedup (invariant I11) covers exact message redelivery; the
//! `payments.order_id` uniqueness in [`crate::repository::find_by_order`]
//! covers a *different* command instance for the same order (e.g. a
//! second `authorize_payment` a caller-level bug re-sent); and the fake
//! provider's own idempotency-key ledger ([`crate::provider`]) covers this
//! handler's own bounded retry loop around a transient provider failure.
//!
//! The provider retry loop runs *inside* the same transaction as the inbox
//! claim and business mutation, unlike the outbox publisher's explicit
//! "never hold a transaction open during I/O" rule — that rule is about
//! real broker network calls; the fake provider here is in-process with no
//! real network I/O, so this project accepts holding the transaction
//! through the (fast, bounded) retry loop to keep the crash-recovery model
//! identical to M04's single-transaction handler rather than introducing a
//! new partial-completion window. See
//! `docs/adr/0009-payments-provider-retry-inside-transaction.md`.

use std::time::{Duration as StdDuration, Instant};

use chrono::{DateTime, Utc};
use contracts::Envelope;
use contracts::payments::{
    AUTHORIZE_PAYMENT_COMMAND_TYPE, AUTHORIZE_PAYMENT_SCHEMA_VERSION, AuthorizePaymentPayload,
    PAYMENT_AGGREGATE_TYPE, PAYMENT_AUTHORIZED_EVENT_TYPE, PAYMENT_AUTHORIZED_SCHEMA_VERSION,
    PAYMENT_FAILED_EVENT_TYPE, PAYMENT_FAILED_SCHEMA_VERSION, PAYMENT_REFUNDED_EVENT_TYPE,
    PAYMENT_REFUNDED_SCHEMA_VERSION, PAYMENTS_EVENTS_TOPIC, PAYMENTS_PRODUCER_NAME,
    PaymentAuthorizedPayload, PaymentFailedPayload, PaymentRefundedPayload,
    REFUND_PAYMENT_COMMAND_TYPE, REFUND_PAYMENT_SCHEMA_VERSION, RefundPaymentPayload,
};
use messaging::{ConsumedRecord, Consumer, Producer};
use persistence::dlq::DlqRecord;
use persistence::inbox::{NewInboxEntry, VersionDecision};
use persistence::outbox::NewOutboxEvent;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use test_support::FaultInjector;
use uuid::Uuid;

use crate::domain::{self, OperationStatus, OperationType, PaymentStatus};
use crate::provider::{PaymentProvider, ProviderOutcome};
use crate::repository;

pub const CONSUMER_NAME: &str = "payments-consumer";
pub const SOURCE_PARTITION: i32 = 0;

/// Fault point (spec section 17: "after consumer DB commit/before offset
/// commit"), identical in shape to inventory's M04 fault point.
pub const FAULT_AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT: &str =
    "payments.after_db_commit_before_offset_commit";

/// Provider-call retry budget (spec section 15 defaults): bounded attempts
/// and elapsed time, full-jitter backoff between attempts.
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

enum AuthorizeResult {
    Outbox(Vec<NewOutboxEvent>),
    RetryExhausted,
}

/// Runs the provider's authorize call to a final outcome or budget
/// exhaustion, then persists the result. The outer `order_id` uniqueness
/// check makes a redelivered command for an already-decided order a no-op
/// (no second provider call, no second outbox event) before any provider
/// interaction happens.
async fn handle_authorize(
    tx: &mut Transaction<'_, Postgres>,
    provider: &dyn PaymentProvider,
    retry_config: &RetryConfig,
    envelope: &Envelope<serde_json::Value>,
    payload: &AuthorizePaymentPayload,
    now: DateTime<Utc>,
) -> Result<AuthorizeResult, anyhow::Error> {
    if repository::find_by_order(tx, payload.order_id)
        .await?
        .is_some()
    {
        return Ok(AuthorizeResult::Outbox(Vec::new()));
    }

    let idempotency_key = domain::authorize_idempotency_key(payload.order_id);
    let start = Instant::now();
    let mut attempts_made: u32 = 0;
    let mut outcome = None;

    while attempts_made < retry_config.max_attempts {
        attempts_made += 1;
        match provider
            .authorize(
                &idempotency_key,
                payload.order_id,
                payload.amount.minor_units,
                &payload.amount.currency,
            )
            .await
        {
            Ok(o) => {
                outcome = Some(o);
                break;
            }
            Err(_transient) => {
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
        }
    }

    let Some(provider_outcome) = outcome else {
        return Ok(AuthorizeResult::RetryExhausted);
    };

    let (op_status, events) = match provider_outcome {
        ProviderOutcome::Authorized { provider_reference } => {
            repository::record_authorized(
                tx,
                payload.payment_id,
                payload.order_id,
                &payload.amount.currency,
                payload.amount.minor_units,
                &provider_reference,
                now,
            )
            .await?;
            let event = build_authorized_event(envelope, payload, &provider_reference);
            (OperationStatus::Succeeded, vec![event])
        }
        ProviderOutcome::Declined { reason_code } => {
            repository::record_declined(
                tx,
                payload.payment_id,
                payload.order_id,
                &payload.amount.currency,
                payload.amount.minor_units,
                &reason_code,
                now,
            )
            .await?;
            let event = build_failed_event(envelope, payload, &reason_code);
            (OperationStatus::Failed, vec![event])
        }
    };

    repository::record_operation(
        tx,
        payload.payment_id,
        OperationType::Authorize,
        &idempotency_key,
        op_status,
        attempts_made as i32,
        now,
    )
    .await?;

    Ok(AuthorizeResult::Outbox(events))
}

/// Refund has no fault points in this milestone's required scope (spec
/// section 17 lists only timeout/decline/response-loss for authorization);
/// idempotency alone is required and proven by the provider's ledger plus
/// [`crate::repository::record_refunded`]'s already-refunded short-circuit.
async fn handle_refund(
    tx: &mut Transaction<'_, Postgres>,
    provider: &dyn PaymentProvider,
    retry_config: &RetryConfig,
    envelope: &Envelope<serde_json::Value>,
    payload: &RefundPaymentPayload,
    now: DateTime<Utc>,
) -> Result<Vec<NewOutboxEvent>, anyhow::Error> {
    let payment = repository::find_by_order(tx, payload.order_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "refund_payment for order {} with no existing payment row",
                payload.order_id
            )
        })?;

    if payment.status == PaymentStatus::Refunded {
        return Ok(Vec::new());
    }

    let idempotency_key = domain::refund_idempotency_key(payload.order_id);
    let provider_reference = payment.provider_reference.clone().unwrap_or_default();
    let started = Instant::now();
    let mut attempts = 0;
    loop {
        attempts += 1;
        match provider
            .refund(&idempotency_key, payload.order_id, &provider_reference)
            .await
        {
            Ok(()) => break,
            Err(_)
                if attempts < retry_config.max_attempts
                    && started.elapsed() < retry_config.max_elapsed =>
            {
                let delay = persistence::outbox::full_jitter_backoff(
                    attempts - 1,
                    retry_config.backoff_base,
                    retry_config.backoff_cap,
                );
                tokio::time::sleep(delay).await;
            }
            Err(_) => return Ok(vec![build_refund_failed_event(envelope, payload)]),
        }
    }

    repository::record_refunded(tx, &payment, now).await?;
    repository::record_operation(
        tx,
        payment.id,
        OperationType::Refund,
        &idempotency_key,
        OperationStatus::Succeeded,
        1,
        now,
    )
    .await?;

    Ok(vec![build_refunded_event(envelope, payload)])
}

fn build_authorized_event(
    envelope: &Envelope<serde_json::Value>,
    payload: &AuthorizePaymentPayload,
    provider_reference: &str,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: PAYMENT_AUTHORIZED_EVENT_TYPE.to_string(),
        schema_version: PAYMENT_AUTHORIZED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: PAYMENTS_PRODUCER_NAME.to_string(),
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 1,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: PaymentAuthorizedPayload {
            order_id: payload.order_id,
            payment_id: payload.payment_id,
            provider_reference: provider_reference.to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 1,
        topic: PAYMENTS_EVENTS_TOPIC.to_string(),
        message_key: payload.order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

fn build_failed_event(
    envelope: &Envelope<serde_json::Value>,
    payload: &AuthorizePaymentPayload,
    reason_code: &str,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: PAYMENT_FAILED_EVENT_TYPE.to_string(),
        schema_version: PAYMENT_FAILED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: PAYMENTS_PRODUCER_NAME.to_string(),
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 1,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: PaymentFailedPayload {
            order_id: payload.order_id,
            payment_id: payload.payment_id,
            reason_code: reason_code.to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 1,
        topic: PAYMENTS_EVENTS_TOPIC.to_string(),
        message_key: payload.order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

fn build_refunded_event(
    envelope: &Envelope<serde_json::Value>,
    payload: &RefundPaymentPayload,
) -> NewOutboxEvent {
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: PAYMENT_REFUNDED_EVENT_TYPE.to_string(),
        schema_version: PAYMENT_REFUNDED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: PAYMENTS_PRODUCER_NAME.to_string(),
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 2,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: PaymentRefundedPayload {
            order_id: payload.order_id,
            payment_id: payload.payment_id,
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 2,
        topic: PAYMENTS_EVENTS_TOPIC.to_string(),
        message_key: payload.order_id.to_string(),
        envelope: serde_json::to_value(&inner).expect("envelope serializes"),
    }
}

fn build_refund_failed_event(
    envelope: &Envelope<serde_json::Value>,
    payload: &RefundPaymentPayload,
) -> NewOutboxEvent {
    use contracts::payments::{
        REFUND_FAILED_EVENT_TYPE, REFUND_FAILED_SCHEMA_VERSION, RefundFailedPayload,
    };
    let event_id = Uuid::now_v7();
    let inner = Envelope {
        event_id,
        event_type: REFUND_FAILED_EVENT_TYPE.to_string(),
        schema_version: REFUND_FAILED_SCHEMA_VERSION,
        occurred_at: Utc::now(),
        producer: PAYMENTS_PRODUCER_NAME.to_string(),
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 2,
        correlation_id: envelope.correlation_id,
        causation_id: envelope.event_id,
        traceparent: None,
        payload: RefundFailedPayload {
            order_id: payload.order_id,
            payment_id: payload.payment_id,
            reason_code: "REFUND_RETRY_BUDGET_EXHAUSTED".to_string(),
        },
    };
    NewOutboxEvent {
        id: event_id,
        aggregate_type: PAYMENT_AGGREGATE_TYPE.to_string(),
        aggregate_id: payload.payment_id,
        aggregate_version: 2,
        topic: PAYMENTS_EVENTS_TOPIC.to_string(),
        message_key: payload.order_id.to_string(),
        envelope: serde_json::to_value(inner).expect("serializes"),
    }
}

/// Runs the eight-step protocol for one record, dispatching on
/// `event_type` since both commands share `payments.commands.v1`. Never
/// returns `Err` for a business-level problem with the message itself
/// (malformed envelope, unsupported schema, hash mismatch, retry-budget
/// exhaustion) — those are DLQ'd and reported as `Poison`; `Err` is
/// reserved for infrastructure failures that should stop the batch.
async fn handle_one(
    pool: &PgPool,
    producer: &dyn Producer,
    provider: &dyn PaymentProvider,
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

    let expected_schema_version = match envelope.event_type.as_str() {
        AUTHORIZE_PAYMENT_COMMAND_TYPE => AUTHORIZE_PAYMENT_SCHEMA_VERSION,
        REFUND_PAYMENT_COMMAND_TYPE => REFUND_PAYMENT_SCHEMA_VERSION,
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

    let outbox_events = if envelope.event_type == AUTHORIZE_PAYMENT_COMMAND_TYPE {
        let payload: AuthorizePaymentPayload =
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
                        format!("payload did not match authorize_payment shape: {e}"),
                    )
                    .await?;
                    return Ok(HandleOutcome::Poison);
                }
            };
        match handle_authorize(&mut tx, provider, retry_config, &envelope, &payload, now).await? {
            AuthorizeResult::Outbox(events) => events,
            AuthorizeResult::RetryExhausted => {
                persistence::inbox::mark_processed(&mut tx, CONSUMER_NAME, envelope.event_id, now)
                    .await?;
                tx.commit().await?;
                publish_dlq(
                    producer,
                    source_topic,
                    &key,
                    record,
                    Some(serde_json::to_value(&envelope).unwrap_or_default()),
                    "PROVIDER_RETRY_BUDGET_EXHAUSTED",
                    format!(
                        "authorize_payment for order {} exhausted its retry budget against the provider",
                        payload.order_id
                    ),
                )
                .await?;
                return Ok(HandleOutcome::Poison);
            }
        }
    } else {
        let payload: RefundPaymentPayload = match serde_json::from_value(envelope.payload.clone()) {
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
                    format!("payload did not match refund_payment shape: {e}"),
                )
                .await?;
                return Ok(HandleOutcome::Poison);
            }
        };
        handle_refund(&mut tx, provider, retry_config, &envelope, &payload, now).await?
    };

    for event in &outbox_events {
        persistence::outbox::insert(&mut tx, now, event).await?;
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
#[allow(clippy::too_many_arguments)]
pub async fn process_available(
    pool: &PgPool,
    consumer: &dyn Consumer,
    producer: &dyn Producer,
    provider: &dyn PaymentProvider,
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

    for record in &records {
        let outcome =
            handle_one(pool, producer, provider, source_topic, record, retry_config).await?;
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
