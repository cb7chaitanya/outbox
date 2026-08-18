//! Shared primitives used by every service: identifiers, money, clocks, and
//! the base error taxonomy. No business rules live here — only vocabulary
//! types that domain crates build on.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wall-clock abstraction so tests can inject deterministic time instead of
/// reading the OS clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Integer minor-units money. Floating point is forbidden for monetary
/// values project-wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    pub currency: Currency,
    pub minor_units: i64,
}

/// Uppercase ISO-4217-shaped currency code, validated for shape only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Currency([u8; 3]);

impl Currency {
    pub fn parse(code: &str) -> Result<Self, DomainError> {
        let bytes = code.as_bytes();
        if bytes.len() != 3 || !bytes.iter().all(u8::is_ascii_uppercase) {
            return Err(DomainError::InvalidCurrency(code.to_string()));
        }
        Ok(Self([bytes[0], bytes[1], bytes[2]]))
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.0).unwrap_or("???"))
    }
}

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new_v7() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

uuid_id!(OrderId);
uuid_id!(EventId);
uuid_id!(CorrelationId);

/// Base error taxonomy shared across services. Service crates extend this
/// with their own domain-specific variants rather than replacing it.
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("invalid currency code: {0}")]
    InvalidCurrency(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn currency_accepts_uppercase_three_letter_codes() {
        assert!(Currency::parse("USD").is_ok());
    }

    #[test]
    fn currency_rejects_lowercase_or_wrong_length() {
        assert!(Currency::parse("usd").is_err());
        assert!(Currency::parse("US").is_err());
    }
}
