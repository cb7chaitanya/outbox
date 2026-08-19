use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryMode {
    /// Direct DB-commit-then-Kafka-publish (spec section 11). Kept
    /// runnable behind this opt-in mode so the M02 failure lab remains
    /// reproducible; no longer the default now that the outbox exists.
    Naive,
    /// Atomic DB-commit-plus-outbox-row, published by a separate worker
    /// (spec section 13). The default since M03.
    Outbox,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub bind_addr: String,
    pub port: u16,
    pub delivery_mode: DeliveryMode,
    pub outbox_batch_size: i64,
    pub outbox_lease_seconds: i64,
    pub outbox_poll_interval_ms: u64,
    pub outbox_backoff_base_ms: u64,
    pub outbox_backoff_cap_ms: u64,
    /// How long the reservation-outcome consumer's fetch may block waiting
    /// for new records before returning an empty batch (rskafka's
    /// `max_wait_ms`).
    pub consumer_max_wait_ms: i32,
    /// How long to sleep between consumer polls when a fetch comes back
    /// empty.
    pub consumer_poll_interval_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".to_string(),
            port: 8081,
            delivery_mode: DeliveryMode::Outbox,
            outbox_batch_size: 20,
            outbox_lease_seconds: 30,
            outbox_poll_interval_ms: 200,
            outbox_backoff_base_ms: 100,
            outbox_backoff_cap_ms: 30_000,
            consumer_max_wait_ms: 1_000,
            consumer_poll_interval_ms: 200,
        }
    }
}

impl Config {
    /// Loads defaults, then overlays `ORDERS_*` environment variables
    /// (e.g. `ORDERS_PORT=8081`), then the shared unprefixed `DELIVERY_MODE`.
    pub fn load() -> anyhow::Result<Self> {
        Figment::from(Serialized::defaults(Config::default()))
            .merge(Env::prefixed("ORDERS_"))
            .merge(Env::raw().only(&["delivery_mode"]))
            .extract()
            .map_err(anyhow::Error::from)
    }

    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.bind_addr, self.port)
    }

    pub fn publisher_config(&self, claimed_by: String) -> persistence::outbox::PublisherConfig {
        persistence::outbox::PublisherConfig {
            claimed_by,
            batch_size: self.outbox_batch_size,
            lease: chrono::Duration::seconds(self.outbox_lease_seconds),
            poll_interval: std::time::Duration::from_millis(self.outbox_poll_interval_ms),
            backoff_base: std::time::Duration::from_millis(self.outbox_backoff_base_ms),
            backoff_cap: std::time::Duration::from_millis(self.outbox_backoff_cap_ms),
        }
    }
}

/// The shared `POSTGRES_*` variables from `.env.example` describe one
/// server with one database per service (spec section 6); this service
/// only ever opens its own `orders` database.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_orders_db: String,
}

impl DatabaseConfig {
    pub fn load() -> anyhow::Result<Self> {
        Figment::new()
            .merge(Env::raw())
            .extract()
            .map_err(anyhow::Error::from)
    }

    pub fn database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.postgres_user,
            self.postgres_password,
            self.postgres_host,
            self.postgres_port,
            self.postgres_orders_db
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessagingConfig {
    pub redpanda_broker: String,
}

impl MessagingConfig {
    pub fn load() -> anyhow::Result<Self> {
        Figment::new()
            .merge(Env::raw())
            .extract()
            .map_err(anyhow::Error::from)
    }
}

/// Failure injection is off by default and must refuse to enable in an
/// environment named `production` (spec section 17).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureInjectionConfig {
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub failure_injection_enabled: bool,
    #[serde(default)]
    pub failure_injection_token: String,
}

impl Default for FailureInjectionConfig {
    fn default() -> Self {
        Self {
            environment: "development".to_string(),
            failure_injection_enabled: false,
            failure_injection_token: String::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("FAILURE_INJECTION_ENABLED=true is refused when ENVIRONMENT=production")]
pub struct FailureInjectionInProductionError;

impl FailureInjectionConfig {
    pub fn load() -> anyhow::Result<Self> {
        let config: Self = Figment::from(Serialized::defaults(Self::default()))
            .merge(Env::raw())
            .extract()?;
        if config.failure_injection_enabled && config.environment == "production" {
            return Err(FailureInjectionInProductionError.into());
        }
        Ok(config)
    }

    pub fn enabled(&self) -> bool {
        self.failure_injection_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env vars are process-global; serialize these tests so they don't
    // stomp on each other when the workspace runs them in parallel.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn delivery_mode_defaults_to_outbox() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: guarded by ENV_LOCK; no other thread reads/writes these
        // vars concurrently within this test binary.
        unsafe {
            std::env::remove_var("DELIVERY_MODE");
        }
        let config = Config::load().expect("config loads with no env override");
        assert_eq!(config.delivery_mode, DeliveryMode::Outbox);
    }

    #[test]
    fn delivery_mode_reads_unprefixed_env_var() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("DELIVERY_MODE", "naive");
        }
        let config = Config::load().expect("config loads with DELIVERY_MODE=naive");
        assert_eq!(config.delivery_mode, DeliveryMode::Naive);
        unsafe {
            std::env::remove_var("DELIVERY_MODE");
        }
    }

    #[test]
    fn failure_injection_refuses_production() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("FAILURE_INJECTION_ENABLED", "true");
            std::env::set_var("ENVIRONMENT", "production");
        }
        let result = FailureInjectionConfig::load();
        unsafe {
            std::env::remove_var("FAILURE_INJECTION_ENABLED");
            std::env::remove_var("ENVIRONMENT");
        }
        assert!(result.is_err());
    }

    #[test]
    fn failure_injection_allowed_outside_production() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("FAILURE_INJECTION_ENABLED", "true");
            std::env::set_var("ENVIRONMENT", "development");
        }
        let result = FailureInjectionConfig::load();
        unsafe {
            std::env::remove_var("FAILURE_INJECTION_ENABLED");
            std::env::remove_var("ENVIRONMENT");
        }
        assert!(result.expect("should load").enabled());
    }
}
