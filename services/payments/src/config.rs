use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub bind_addr: String,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".to_string(),
            port: 8083,
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
}
