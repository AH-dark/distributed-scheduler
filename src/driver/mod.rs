use std::fmt::Debug;

mod utils;

#[cfg(feature = "driver-redis")]
pub mod redis;

#[cfg(feature = "driver-redis")]
pub mod redis_zset;

#[cfg(feature = "driver-etcd")]
pub mod etcd;

#[async_trait::async_trait]
pub trait Driver: Send + Sync + Debug {
    fn node_id(&self) -> String;
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
