use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    for (name, env, migrator) in [
        (
            "orders",
            "ORDERS_DATABASE_URL",
            sqlx::migrate!("../../services/orders/migrations"),
        ),
        (
            "inventory",
            "INVENTORY_DATABASE_URL",
            sqlx::migrate!("../../services/inventory/migrations"),
        ),
        (
            "payments",
            "PAYMENTS_DATABASE_URL",
            sqlx::migrate!("../../services/payments/migrations"),
        ),
        (
            "fulfilment",
            "FULFILMENT_DATABASE_URL",
            sqlx::migrate!("../../services/fulfilment/migrations"),
        ),
    ] {
        let url = std::env::var(env)?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;
        migrator.run(&pool).await?;
        println!("migrated {name}");
    }
    Ok(())
}
