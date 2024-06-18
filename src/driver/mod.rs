mod utils;

#[cfg(feature = "redis_driver")]
pub mod redis;

#[async_trait::async_trait]
pub trait Driver: Send + Sync {
    async fn init(&mut self, service_name: String) -> Result<(), Box<dyn std::error::Error>>;
    async fn node_id(&self) -> Result<&str, Box<dyn std::error::Error>>;
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>>;
}
