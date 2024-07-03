use std::fmt::Debug;

mod utils;

#[cfg(feature = "redis_driver")]
pub mod redis;

#[cfg(feature = "redis_driver")]
pub mod redis_zset;

#[async_trait::async_trait]
pub trait Driver: Send + Sync + Debug {
    fn node_id(&self) -> String;
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>>;
}
