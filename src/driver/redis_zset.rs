use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use super::{utils, Driver};
use crate::driver::redis::RedisDriver;
use redis::aio::ConnectionLike;
use redis::AsyncCommands;

const DEFAULT_TIMEOUT: u64 = 3;

#[derive(Clone, Debug)]
pub struct RedisZSetDriver<C>
where
    C: ConnectionLike,
{
    con: C,

    service_name: String,
    node_id: String,
    started: Arc<AtomicBool>,
    timeout: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Empty service name")]
    EmptyServiceName,
    #[error("Empty node id")]
    EmptyNodeId,
}

impl<C> RedisZSetDriver<C>
where
    C: ConnectionLike,
{
    pub async fn new(con: C, service_name: &str, node_id: &str) -> Result<Self, Error> {
        if service_name.is_empty() {
            return Err(Error::EmptyServiceName);
        }

        if node_id.is_empty() {
            return Err(Error::EmptyNodeId);
        }

        Ok(Self {
            con,
            service_name: service_name.into(),
            node_id: utils::get_key_prefix(service_name) + node_id,
            started: Arc::new(AtomicBool::new(false)),
            timeout: DEFAULT_TIMEOUT,
        })
    }

    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }
}


#[async_trait::async_trait]
impl<C> Driver for RedisZSetDriver<C>
where
    C: ConnectionLike + Send + Sync + Clone + 'static,
{
    fn node_id(&self) -> String {
        self.node_id.clone()
    }

    /// Get the list of nodes from the redis server, use `ZRANGEBYSCORE` to get the latest nodes
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let key = utils::get_zset_key(&self.service_name);

        let mut con = self.con.clone();
        let min = (chrono::Utc::now() - chrono::Duration::seconds(self.timeout as i64)).timestamp(); // current timestamp - timeout
        let nodes: Vec<String> = con.zrangebyscore(key, min, "+inf").await?;
        Ok(nodes)
    }

    /// Start a routine to send the heartbeat to the redis server
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // check if the driver has already started
        if self.started.load(std::sync::atomic::Ordering::SeqCst) {
            log::warn!("Driver has already started");
            return Ok(());
        }

        // set the driver as started
        self.started.store(true, std::sync::atomic::Ordering::SeqCst);

        // start the heartbeat
        tokio::spawn({
            let con = self.con.clone();
            let service_name = self.service_name.clone();
            let node_id = self.node_id.clone();
            let started = self.started.clone();

            async move {
                heartbeat(&service_name, &node_id, con, started)
                    .await
                    .expect("Failed to start scheduler driver heartbeat")
            }
        });

        Ok(())
    }
}

impl<C> Drop for RedisZSetDriver<C>
where
    C: ConnectionLike,
{
    fn drop(&mut self) {
        self.started.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Register the node in the redis.
/// Use redis command `ZADD` to add the node to the zset.
///
/// # Arguments
///
/// * `service_name` - The name of the service
/// * `node_id` - The id of the node
/// * `con` - The redis connection
/// * `time` - The time to register the node
///
async fn register_node<C: ConnectionLike + Send>(service_name: &str, node_id: &str, con: &mut C, time: i64) -> Result<(), Box<dyn std::error::Error>> {
    con.zadd(utils::get_zset_key(service_name), node_id, time).await?;
    Ok(())
}

/// Heartbeat function to keep the node alive
///
/// # Arguments
///
/// * `service_name` - The name of the service
/// * `node_id` - The id of the node
/// * `timeout` - The timeout of the node
/// * `con` - The redis connection
/// * `started` - The atomic bool to check if the driver has started
///
/// # Returns
///
/// * `Result<(), Box<dyn std::error::Error>` - The result of the function
///
async fn heartbeat<C: ConnectionLike + Send>(service_name: &str, node_id: &str, con: C, started: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let mut con = con;
    let mut error_time = 0;

    log::debug!("Started heartbeat");

    loop {
        // check if the driver has stopped
        if !started.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }

        // tick the interval
        interval.tick().await;

        // register the node
        register_node(service_name, node_id, &mut con, chrono::Utc::now().timestamp()).await
            .map_err(|e| {
                error_time += 1;
                log::error!("Failed to register node: {:?}", e);
            })
            .ok();

        // check if the error time is greater than 5
        if error_time >= 5 {
            panic!("Failed to register node 5 times, stopping heartbeat");
        }
    }

    log::info!("Heartbeat stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[tokio::test]
    async fn test_register_node_success() {
        let service_name = "test-service";
        let node_id = "test-node";
        let ts = chrono::Utc::now().timestamp();

        let mut mock_con = MockRedisConnection::new(vec![
            MockCmd::new(
                redis::cmd("ZADD").arg(utils::get_zset_key(service_name)).arg(ts).arg(node_id),
                Ok(redis::Value::Okay),
            ),
        ]);

        // Perform the node registration
        let result = register_node(service_name, node_id, &mut mock_con, ts).await;

        assert!(result.is_ok(), "Register node should be successful: {}", result.unwrap_err());
    }

    #[tokio::test]
    async fn test_get_nodes_success() {
        let service_name = "test-service";
        let node_id = "test-node";

        let keys = ["node1", "node2", "node3"];
        let keys_as_redis_value: Vec<redis::Value> = keys.iter().map(|k| redis::Value::BulkString(k.as_bytes().to_vec())).collect();

        let mock_con = MockRedisConnection::new(vec![
            MockCmd::new(
                redis::cmd("ZRANGEBYSCORE")
                    .arg(utils::get_zset_key(service_name))
                    .arg((chrono::Utc::now() - chrono::Duration::seconds(DEFAULT_TIMEOUT as i64)).timestamp())
                    .arg("+inf"),
                Ok(redis::Value::Array(keys_as_redis_value)),
            )
        ]);

        // Perform the node registration
        let driver = RedisZSetDriver::new(mock_con, service_name, node_id).await.unwrap();
        let result = driver.get_nodes().await;

        assert!(result.is_ok(), "Get nodes should be successful: {}", result.unwrap_err());
        assert_eq!(result.unwrap(), keys, "The nodes should match");
    }
}

