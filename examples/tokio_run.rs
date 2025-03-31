use distributed_scheduler::{cron, driver, node_pool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let np = node_pool::NodePool::new(driver::local::LocalDriver).await?;
    let cron = cron::Cron::new(np).await;

    tracing::info!("Adding job");
    cron.add_job("job1", "* * * * * *".parse().unwrap(), || {
        tracing::info!("Running job 1: {}", chrono::Utc::now());
    })
    .await?;

    tracing::info!("Starting cron");
    let _cron = cron.clone();
    tokio::spawn(async move { _cron.start().await.unwrap() });

    cron.add_job("job2", "* * * * * *".parse().unwrap(), || {
        tracing::info!("Running job 2: {}", chrono::Utc::now());
    })
    .await?;

    cron.remove_job("job1").await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}
