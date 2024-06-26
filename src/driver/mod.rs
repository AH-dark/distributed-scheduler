use std::fmt::Debug;

mod utils;

#[cfg(feature = "redis_driver")]
pub mod redis;

#[async_trait::async_trait]
pub(crate) trait Driver: Send + Sync + Debug {
    async fn init(&mut self, service_name: String) -> Result<(), Box<dyn std::error::Error>>;
    fn node_id(&self) -> String;
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>>;
}
