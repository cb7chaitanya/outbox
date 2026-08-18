//! The `FaultInjector` port (spec section 17): a named, in-memory fault
//! registry that production code calls into at specific, deliberate points
//! instead of scattering sleeps or panics through domain logic. Disabled by
//! default; services only mount HTTP controls for it, and only ever call
//! [`FaultInjector::maybe_fail`], when `FAILURE_INJECTION_ENABLED=true` and
//! the running environment is not named `production`.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// One named fault's configuration: how many more times it should fire, an
/// optional subject filter (e.g. an order ID — only matching calls trigger),
/// and an optional fixed delay applied before the fault takes effect. This
/// project's retry/backoff jitter (spec section 15) needs a seeded RNG;
/// this delay is a plain fixed wait for deterministic fault-lab timing, not
/// that jitter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FaultConfig {
    pub fail_next: u32,
    #[serde(default)]
    pub subject_filter: Option<String>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
}

/// Returned by [`FaultInjector::maybe_fail`] when a fault fires, so callers
/// can turn it into a service-appropriate error response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultTriggered {
    pub name: String,
}

#[derive(Debug, Default)]
pub struct FaultInjector {
    faults: Mutex<HashMap<String, FaultConfig>>,
}

impl FaultInjector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers or replaces a named fault's configuration.
    pub fn configure(&self, name: &str, config: FaultConfig) {
        self.faults
            .lock()
            .expect("fault injector lock poisoned")
            .insert(name.to_string(), config);
    }

    /// Removes every configured fault.
    pub fn clear(&self) {
        self.faults
            .lock()
            .expect("fault injector lock poisoned")
            .clear();
    }

    /// Snapshot of the currently configured faults, for the read-side of
    /// the `/_test/faults` control endpoints and for tests.
    pub fn snapshot(&self) -> HashMap<String, FaultConfig> {
        self.faults
            .lock()
            .expect("fault injector lock poisoned")
            .clone()
    }

    /// Call at a named fault point. `subject` is compared against the
    /// configured `subject_filter` when one is set (e.g. an order ID) so a
    /// test can target a single in-flight request without affecting
    /// concurrent traffic. On a match, decrements the remaining count
    /// (removing the entry once exhausted), waits `delay_ms` if configured,
    /// then returns `Err(FaultTriggered)`.
    pub async fn maybe_fail(
        &self,
        name: &str,
        subject: Option<&str>,
    ) -> Result<(), FaultTriggered> {
        let delay = {
            let mut guard = self.faults.lock().expect("fault injector lock poisoned");
            let Some(config) = guard.get_mut(name) else {
                return Ok(());
            };
            if let Some(filter) = &config.subject_filter
                && subject != Some(filter.as_str())
            {
                return Ok(());
            }
            if config.fail_next == 0 {
                return Ok(());
            }
            config.fail_next -= 1;
            let delay = config.delay_ms;
            if config.fail_next == 0 {
                guard.remove(name);
            }
            delay
        };
        if let Some(delay_ms) = delay {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        Err(FaultTriggered {
            name: name.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fires_exactly_fail_next_times_then_stops() {
        let injector = FaultInjector::new();
        injector.configure(
            "test.point",
            FaultConfig {
                fail_next: 2,
                subject_filter: None,
                delay_ms: None,
            },
        );
        assert!(injector.maybe_fail("test.point", None).await.is_err());
        assert!(injector.maybe_fail("test.point", None).await.is_err());
        assert!(injector.maybe_fail("test.point", None).await.is_ok());
    }

    #[tokio::test]
    async fn only_fires_for_matching_subject() {
        let injector = FaultInjector::new();
        injector.configure(
            "test.point",
            FaultConfig {
                fail_next: 5,
                subject_filter: Some("order-a".to_string()),
                delay_ms: None,
            },
        );
        assert!(
            injector
                .maybe_fail("test.point", Some("order-b"))
                .await
                .is_ok()
        );
        assert!(
            injector
                .maybe_fail("test.point", Some("order-a"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unconfigured_fault_never_fires() {
        let injector = FaultInjector::new();
        assert!(injector.maybe_fail("unknown", None).await.is_ok());
    }

    #[tokio::test]
    async fn clear_removes_all_configured_faults() {
        let injector = FaultInjector::new();
        injector.configure(
            "test.point",
            FaultConfig {
                fail_next: 1,
                subject_filter: None,
                delay_ms: None,
            },
        );
        injector.clear();
        assert!(injector.maybe_fail("test.point", None).await.is_ok());
    }
}
