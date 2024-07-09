mod utils;

#[cfg(feature = "driver-redis")]
pub mod redis;

#[cfg(feature = "driver-redis")]
pub mod redis_zset;

#[cfg(feature = "driver-etcd")]
pub mod etcd;

#[async_trait::async_trait]
pub trait Driver {
    /// Get the local node id.
    fn node_id(&self) -> String;
    /// Get the node list in the cluster
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    /// Start the driver, blocking the current thread.
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
