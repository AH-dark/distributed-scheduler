mod utils;

#[cfg(feature = "driver-redis")]
/// Driver based on Redis Scan
pub mod redis;

#[cfg(feature = "driver-redis")]
/// Driver based on Redis ZSet
pub mod redis_zset;

#[cfg(feature = "driver-etcd")]
/// Driver based on Etcd
pub mod etcd;

/// Driver based on local node only
pub mod local;

#[async_trait::async_trait]
/// The driver trait, which defines the node list management interface.
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
