mod config;
mod http;

use config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing("fulfilment");

    let config = Config::load()?;
    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(addr = %config.socket_addr(), "orders listening");

    axum::serve(listener, http::router())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install ctrl-c handler");
}
