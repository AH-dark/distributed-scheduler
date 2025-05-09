use std::{
    fmt::Debug,
    sync::{atomic::AtomicBool, Arc},
};

use redis::{aio::ConnectionLike, AsyncCommands};

use super::{utils, Driver};

const DEFAULT_TIMEOUT: u64 = 3;

#[derive(Clone)]
pub struct RedisDriver<C>
where
    C: ConnectionLike,
{
    con: C,

    service_name: String,
    node_id: String,
    started: Arc<AtomicBool>,
    timeout: u64,
}

impl<C> Debug for RedisDriver<C>
where
    C: ConnectionLike,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RedisDriver")
            .field("service_name", &self.service_name)
            .field("node_id", &self.node_id)
            .field("started", &self.started)
            .field("timeout", &self.timeout)
            .finish()
    }
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

impl<C> RedisDriver<C>
where
    C: ConnectionLike,
{
    pub async fn new(
        con: C,
        service_name: &str,
        node_id: &str,
    ) -> Result<Self, Error> {
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

    pub fn with_timeout(
        mut self,
        timeout: u64,
    ) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn timeout(&self) -> u64 {
        self.timeout
    }
}

#[async_trait::async_trait]
impl<C> Driver for RedisDriver<C>
where
    C: ConnectionLike + Send + Sync + Clone + 'static,
{
    type Error = Error;

    fn node_id(&self) -> String {
        self.node_id.clone()
    }

    /// Scan the redis server to get the nodes
    async fn get_nodes(&self) -> Result<Vec<String>, Self::Error> {
        let pattern = utils::get_key_prefix(&self.service_name) + "*";

        let mut con = self.con.clone();
        let mut res = con.scan_match(pattern).await?;

        let mut nodes: Vec<String> = Vec::new();
        while let Some(key) = res.next_item().await {
            nodes.push(key);
        }
        Ok(nodes)
    }

    /// Start a routine to send heartbeat to the redis server
    async fn start(&mut self) -> Result<(), Self::Error> {
        // check if the driver has already started
        if self.started.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!("Driver has already started");
            return Ok(());
        }

        // set the driver as started
        self.started.store(true, std::sync::atomic::Ordering::SeqCst);

        // start the heartbeat
        tokio::spawn({
            let con = self.con.clone();
            let node_id = self.node_id.clone();
            let timeout = self.timeout;
            let started = self.started.clone();

            async move {
                heartbeat(&node_id, timeout, con, started)
                    .await
                    .expect("Failed to start scheduler driver heartbeat")
            }
        });

        Ok(())
    }
}

impl<C> Drop for RedisDriver<C>
where
    C: ConnectionLike,
{
    fn drop(&mut self) {
        self.started.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Register the node in the redis
///
/// # Arguments
///
/// * `node_id` - The id of the node
/// * `timeout` - The timeout of the node
/// * `con` - The redis connection
async fn register_node<C: ConnectionLike + Send + Sync>(
    node_id: &str,
    timeout: u64,
    con: &mut C,
) -> Result<(), redis::RedisError> {
    con.set_ex(node_id, node_id, timeout).await?;
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
async fn heartbeat<C: ConnectionLike + Send + Sync>(
    node_id: &str,
    timeout: u64,
    con: C,
    started: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let mut con = con;
    let mut error_time = 0;

    tracing::debug!("Started heartbeat");

    loop {
        // check if the driver has stopped
        if !started.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }

        // tick the interval
        interval.tick().await;

        // register the node
        register_node(node_id, timeout, &mut con)
            .await
            .map_err(|e| {
                error_time += 1;
                tracing::error!("Failed to register node: {:?}", e);
            })
            .ok();

        // check if the error time is greater than 5
        if error_time >= 5 {
            panic!("Failed to register node 5 times, stopping heartbeat");
        }
    }

    tracing::info!("Heartbeat stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[tokio::test]
    async fn test_register_node_success() {
        let node_id = "test-node";
        let timeout = 10_u64;

        let mut mock_con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("SETEX").arg(node_id).arg(timeout as usize).arg(node_id),
            Ok(redis::Value::Okay),
        )]);

        // Perform the node registration
        let result = register_node(node_id, timeout, &mut mock_con).await;

        assert!(result.is_ok(), "Node registration should be successful");
    }

    #[tokio::test]
    async fn test_get_nodes_success() {
        let service_name = "test-service";
        let node_id = "test-node";
        let pattern = utils::get_key_prefix(service_name) + "*";

        let keys = ["test-service-node1", "test-service-node2", "test-service-node3"];
        let keys_as_redis_values = keys
            .iter()
            .map(|k| redis::Value::BulkString(k.to_string().into_bytes()))
            .collect::<Vec<_>>();

        let mock_con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("SCAN").arg("0").arg("MATCH").arg(&pattern),
            Ok(redis::Value::Array(keys_as_redis_values)),
        )]);

        // Perform the node registration
        let driver = RedisDriver::new(mock_con, service_name, node_id).await.unwrap();
        let result = driver.get_nodes().await;

        assert!(result.is_ok(), "Get nodes should be successful");
        assert_eq!(result.unwrap(), keys, "The nodes should match");
    }
}
