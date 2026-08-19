//! A fake external payment provider (spec section 9: "The fake provider
//! must itself honor operation idempotency keys.") and the fault points
//! from section 17 ("payment provider timeout, decline, and
//! success-response loss").
//!
//! [`FakeProvider`] keeps its own in-process ledger keyed by caller-supplied
//! idempotency key, independent of this service's `payment_operations`
//! table (which is *this service's* bookkeeping of what it asked the
//! provider to do, not the provider's own state). A call whose idempotency
//! key is already in the ledger returns the stored result without doing
//! any new work — real double-charging is impossible regardless of how
//! many times the caller retries with the same key. [`real_authorize_calls`]
//! and [`real_refund_calls`] only count calls that actually did new work,
//! so a test can prove a retried operation only ran once for real.
//!
//! Fault ordering inside [`FakeProvider::authorize`] matters: the ledger
//! check happens first (so a redelivered idempotency key never re-triggers
//! a fault), then timeout, then decline, then success/response-loss. A
//! timeout fires *before* anything is recorded — the next call with the
//! same key does fresh work, simulating a call that timed out before the
//! provider itself committed anything. A response-loss fault fires *after*
//! the result is recorded — the next call with the same key replays the
//! cached result, simulating a call that the provider completed but whose
//! response never reached the caller.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use domain_common::ErrorClass;
use test_support::FaultInjector;
use uuid::Uuid;

pub const FAULT_PROVIDER_TIMEOUT: &str = "payments.provider.timeout";
pub const FAULT_PROVIDER_DECLINE: &str = "payments.provider.decline";
pub const FAULT_PROVIDER_RESPONSE_LOST: &str = "payments.provider.success_response_lost";
pub const FAULT_PROVIDER_REFUND_TIMEOUT: &str = "payments.provider.refund_timeout";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderOutcome {
    Authorized { provider_reference: String },
    Declined { reason_code: String },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ProviderError {
    /// Covers both a genuine timeout and a success whose response was
    /// lost in transit — from the caller's side these are indistinguishable
    /// and both call for the same response: retry with the same
    /// idempotency key. Section 15 classifies both as `Transient`.
    #[error("payment provider unavailable: {0}")]
    Unavailable(String),
}

impl ProviderError {
    pub fn error_class(&self) -> ErrorClass {
        match self {
            ProviderError::Unavailable(_) => ErrorClass::Transient,
        }
    }
}

#[async_trait::async_trait]
pub trait PaymentProvider: Send + Sync {
    async fn authorize(
        &self,
        idempotency_key: &str,
        order_id: Uuid,
        amount_minor: i64,
        currency: &str,
    ) -> Result<ProviderOutcome, ProviderError>;

    /// Idempotent: a second call with an idempotency key already in the
    /// refund ledger returns `Ok(())` without re-executing the refund.
    async fn refund(
        &self,
        idempotency_key: &str,
        order_id: Uuid,
        provider_reference: &str,
    ) -> Result<(), ProviderError>;
}

#[derive(Default)]
pub struct FakeProvider {
    fault_injector: std::sync::Arc<FaultInjector>,
    authorize_ledger: Mutex<HashMap<String, ProviderOutcome>>,
    refund_ledger: Mutex<HashMap<String, ()>>,
    real_authorize_calls: AtomicU64,
    real_refund_calls: AtomicU64,
}

impl FakeProvider {
    pub fn new(fault_injector: std::sync::Arc<FaultInjector>) -> Self {
        Self {
            fault_injector,
            ..Default::default()
        }
    }

    /// Number of authorize calls that performed new work (as opposed to
    /// replaying a cached idempotent result).
    pub fn real_authorize_calls(&self) -> u64 {
        self.real_authorize_calls.load(Ordering::Relaxed)
    }

    /// Number of refund calls that performed new work.
    pub fn real_refund_calls(&self) -> u64 {
        self.real_refund_calls.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl PaymentProvider for FakeProvider {
    async fn authorize(
        &self,
        idempotency_key: &str,
        _order_id: Uuid,
        _amount_minor: i64,
        _currency: &str,
    ) -> Result<ProviderOutcome, ProviderError> {
        if let Some(existing) = self
            .authorize_ledger
            .lock()
            .expect("authorize ledger lock poisoned")
            .get(idempotency_key)
            .cloned()
        {
            return Ok(existing);
        }

        if self
            .fault_injector
            .maybe_fail(FAULT_PROVIDER_TIMEOUT, Some(idempotency_key))
            .await
            .is_err()
        {
            // Nothing recorded: a genuine timeout, safe to retry fresh.
            return Err(ProviderError::Unavailable(
                "simulated provider timeout".to_string(),
            ));
        }

        if self
            .fault_injector
            .maybe_fail(FAULT_PROVIDER_DECLINE, Some(idempotency_key))
            .await
            .is_err()
        {
            let outcome = ProviderOutcome::Declined {
                reason_code: "CARD_DECLINED".to_string(),
            };
            self.authorize_ledger
                .lock()
                .expect("authorize ledger lock poisoned")
                .insert(idempotency_key.to_string(), outcome.clone());
            self.real_authorize_calls.fetch_add(1, Ordering::Relaxed);
            return Ok(outcome);
        }

        let response_lost = self
            .fault_injector
            .maybe_fail(FAULT_PROVIDER_RESPONSE_LOST, Some(idempotency_key))
            .await
            .is_err();

        let outcome = ProviderOutcome::Authorized {
            provider_reference: format!("prov_{}", Uuid::now_v7()),
        };
        self.authorize_ledger
            .lock()
            .expect("authorize ledger lock poisoned")
            .insert(idempotency_key.to_string(), outcome.clone());
        self.real_authorize_calls.fetch_add(1, Ordering::Relaxed);

        if response_lost {
            return Err(ProviderError::Unavailable(
                "simulated response loss after commit".to_string(),
            ));
        }
        Ok(outcome)
    }

    async fn refund(
        &self,
        idempotency_key: &str,
        _order_id: Uuid,
        _provider_reference: &str,
    ) -> Result<(), ProviderError> {
        if self
            .refund_ledger
            .lock()
            .expect("refund ledger lock poisoned")
            .contains_key(idempotency_key)
        {
            return Ok(());
        }
        if self
            .fault_injector
            .maybe_fail(FAULT_PROVIDER_REFUND_TIMEOUT, Some(idempotency_key))
            .await
            .is_err()
        {
            return Err(ProviderError::Unavailable(
                "simulated refund timeout".to_string(),
            ));
        }
        self.refund_ledger
            .lock()
            .expect("refund ledger lock poisoned")
            .insert(idempotency_key.to_string(), ());
        self.real_refund_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_support::FaultConfig;

    fn provider() -> FakeProvider {
        FakeProvider::new(std::sync::Arc::new(FaultInjector::new()))
    }

    #[tokio::test]
    async fn authorize_succeeds_and_is_idempotent_on_replay() {
        let p = provider();
        let order_id = Uuid::now_v7();
        let first = p.authorize("key-1", order_id, 1000, "USD").await.unwrap();
        let second = p.authorize("key-1", order_id, 1000, "USD").await.unwrap();
        assert_eq!(first, second);
        assert_eq!(p.real_authorize_calls(), 1);
    }

    #[tokio::test]
    async fn timeout_then_retry_authorizes_once_for_real() {
        let p = provider();
        p.fault_injector.configure(
            FAULT_PROVIDER_TIMEOUT,
            FaultConfig {
                fail_next: 1,
                subject_filter: None,
                delay_ms: None,
            },
        );
        let order_id = Uuid::now_v7();
        assert!(p.authorize("key-2", order_id, 1000, "USD").await.is_err());
        let outcome = p.authorize("key-2", order_id, 1000, "USD").await.unwrap();
        assert!(matches!(outcome, ProviderOutcome::Authorized { .. }));
        assert_eq!(p.real_authorize_calls(), 1);
    }

    #[tokio::test]
    async fn response_lost_then_retry_replays_the_same_authorization() {
        let p = provider();
        p.fault_injector.configure(
            FAULT_PROVIDER_RESPONSE_LOST,
            FaultConfig {
                fail_next: 1,
                subject_filter: None,
                delay_ms: None,
            },
        );
        let order_id = Uuid::now_v7();
        assert!(p.authorize("key-3", order_id, 1000, "USD").await.is_err());
        let outcome = p.authorize("key-3", order_id, 1000, "USD").await.unwrap();
        assert!(matches!(outcome, ProviderOutcome::Authorized { .. }));
        assert_eq!(
            p.real_authorize_calls(),
            1,
            "the lost-response call already committed the result; the retry must be a cache hit"
        );
    }

    #[tokio::test]
    async fn decline_is_a_business_outcome_not_an_error() {
        let p = provider();
        p.fault_injector.configure(
            FAULT_PROVIDER_DECLINE,
            FaultConfig {
                fail_next: 1,
                subject_filter: None,
                delay_ms: None,
            },
        );
        let order_id = Uuid::now_v7();
        let outcome = p.authorize("key-4", order_id, 1000, "USD").await.unwrap();
        assert!(matches!(outcome, ProviderOutcome::Declined { .. }));
    }

    #[tokio::test]
    async fn refund_is_idempotent() {
        let p = provider();
        let order_id = Uuid::now_v7();
        p.refund("refund-1", order_id, "prov_ref").await.unwrap();
        p.refund("refund-1", order_id, "prov_ref").await.unwrap();
        assert_eq!(p.real_refund_calls(), 1);
    }
}
