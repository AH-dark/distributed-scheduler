use std::error::Error;

use distributed_scheduler::{cron, driver, node_pool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let np = node_pool::NodePool::new(driver::local::LocalDriver).await?;
    let cron = cron::Cron::new(np).await;

    log::info!("Adding job");
    cron.add_job("test", "* * * * * *".parse().unwrap(), || {
        log::info!("Running job: {}", chrono::Utc::now());
    })
    .await?;

    log::info!("Starting cron");
    cron.start().await;

    Ok(())
}
