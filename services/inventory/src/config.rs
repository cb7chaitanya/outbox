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
            port: 8082,
        }
    }
}

impl Config {
    /// Loads defaults, then overlays `INVENTORY_*` environment variables
    /// (e.g. `INVENTORY_PORT=8082`).
    pub fn load() -> anyhow::Result<Self> {
        Figment::from(Serialized::defaults(Config::default()))
            .merge(Env::prefixed("INVENTORY_"))
            .extract()
            .map_err(anyhow::Error::from)
    }

    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.bind_addr, self.port)
    }
}
