mod utils;

#[cfg(feature = "driver-etcd")]
/// Driver based on Etcd
pub mod etcd;
/// Driver based on local node only
pub mod local;
#[cfg(feature = "driver-redis")]
/// Driver based on Redis Scan
pub mod redis;
#[cfg(feature = "driver-redis")]
/// Driver based on Redis ZSet
pub mod redis_zset;

#[async_trait::async_trait]
/// The driver trait, which defines the node list management interface.
pub trait Driver {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get the local node id.
    fn node_id(&self) -> String;
    /// Get the node list in the cluster
    async fn get_nodes(&self) -> Result<Vec<String>, Self::Error>;

    /// Start the driver, blocking the current thread.
    async fn start(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
