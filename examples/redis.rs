use distributed_scheduler::{cron, driver, node_pool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let rdb = redis::Client::open("redis://localhost:6379").unwrap();

    let driver = driver::redis::RedisDriver::new(
        rdb.get_multiplexed_tokio_connection().await?,
        "example-service",
        &uuid::Uuid::new_v4().to_string(),
    )
    .await?
    .with_timeout(3);
    let np = node_pool::NodePool::new(driver).await?;
    let cron = cron::Cron::new(np).await;

    tracing::info!("Adding job");
    cron.add_job("test", "* * * * * *".parse().unwrap(), || {
        tracing::info!("Running job: {}", chrono::Utc::now());
    })
    .await?;

    tracing::info!("Starting cron");
    cron.start().await;

    Ok(())
}
