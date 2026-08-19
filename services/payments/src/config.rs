use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub bind_addr: String,
    pub port: u16,
    pub outbox_batch_size: i64,
    pub outbox_lease_seconds: i64,
    pub outbox_poll_interval_ms: u64,
    pub outbox_backoff_base_ms: u64,
    pub outbox_backoff_cap_ms: u64,
    /// How long the consumer's fetch may block waiting for new records
    /// before returning an empty batch (rskafka's `max_wait_ms`).
    pub consumer_max_wait_ms: i32,
    /// How long to sleep between consumer polls when a fetch comes back
    /// empty.
    pub consumer_poll_interval_ms: u64,
    /// Provider-call retry budget (spec section 15 defaults): bounded
    /// attempts and elapsed time, full-jitter backoff between them.
    pub provider_retry_max_attempts: u32,
    pub provider_retry_max_elapsed_secs: u64,
    pub provider_retry_backoff_base_ms: u64,
    pub provider_retry_backoff_cap_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".to_string(),
            port: 8083,
            outbox_batch_size: 20,
            outbox_lease_seconds: 30,
            outbox_poll_interval_ms: 200,
            outbox_backoff_base_ms: 100,
            outbox_backoff_cap_ms: 30_000,
            consumer_max_wait_ms: 1_000,
            consumer_poll_interval_ms: 200,
            provider_retry_max_attempts: 8,
            provider_retry_max_elapsed_secs: 600,
            provider_retry_backoff_base_ms: 100,
            provider_retry_backoff_cap_ms: 30_000,
        }
    }
}

impl Config {
    /// Loads defaults, then overlays `PAYMENTS_*` environment variables
    /// (e.g. `PAYMENTS_PORT=8083`).
    pub fn load() -> anyhow::Result<Self> {
        Figment::from(Serialized::defaults(Config::default()))
            .merge(Env::prefixed("PAYMENTS_"))
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

    pub fn provider_retry_config(&self) -> crate::consumer::RetryConfig {
        crate::consumer::RetryConfig {
            max_attempts: self.provider_retry_max_attempts,
            max_elapsed: std::time::Duration::from_secs(self.provider_retry_max_elapsed_secs),
            backoff_base: std::time::Duration::from_millis(self.provider_retry_backoff_base_ms),
            backoff_cap: std::time::Duration::from_millis(self.provider_retry_backoff_cap_ms),
        }
    }
}

/// The shared `POSTGRES_*` variables from `.env.example` describe one
/// server with one database per service (spec section 6); this service
/// only ever opens its own `payments` database.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_payments_db: String,
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
            self.postgres_payments_db
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
