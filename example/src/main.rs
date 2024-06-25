use std::error::Error;

use distributed_scheduler::{cron, driver, node_pool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let rdb = redis::Client::open("redis://localhost:6379").unwrap();

    let driver = driver::redis::RedisDriver::new(rdb).await?;
    let np = node_pool::NodePool::new(driver);
    let cron = cron::Cron::new(np).await?;

    cron.add_job("test", "1/10 * * * * *".parse().unwrap(), || {
        println!("Hello, I run every 10 seconds");
    }).await?;

    Ok(())
}
