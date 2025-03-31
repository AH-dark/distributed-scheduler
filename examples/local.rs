use distributed_scheduler::{cron, driver, node_pool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let np = node_pool::NodePool::new(driver::local::LocalDriver).await?;
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
