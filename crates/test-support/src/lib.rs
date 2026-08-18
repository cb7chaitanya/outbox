//! Deterministic test fixtures: fake clock now; testcontainers wiring,
//! fault-injection controls, and the bounded `eventually` polling helper
//! land alongside the milestones that first need them (M02+).

use std::sync::Mutex;

use chrono::{DateTime, Utc};
use domain_common::Clock;

pub mod fault;
pub use fault::{FaultConfig, FaultInjector, FaultTriggered};

/// A `Clock` whose value only advances when told to, for deterministic
/// invariant tests (spec section 18).
pub struct FakeClock(Mutex<DateTime<Utc>>);

impl FakeClock {
    pub fn at(t: DateTime<Utc>) -> Self {
        Self(Mutex::new(t))
    }

    pub fn advance(&self, delta: chrono::Duration) {
        let mut guard = self.0.lock().expect("fake clock lock poisoned");
        *guard += delta;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("fake clock lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_clock_only_advances_when_told() {
        let start = Utc::now();
        let clock = FakeClock::at(start);
        assert_eq!(clock.now(), start);
        clock.advance(chrono::Duration::seconds(5));
        assert_eq!(clock.now(), start + chrono::Duration::seconds(5));
    }
}
