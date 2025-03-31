use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use redis::{aio::ConnectionLike, AsyncCommands};

use super::{utils, Driver};

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
    notify: Arc<tokio::sync::Notify>,
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
            notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub fn with_timeout(
        mut self,
        timeout: u64,
    ) -> Self {
        self.timeout = timeout;
        self
    }

    fn shutdown(&self) {
        self.started.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl<C> Driver for RedisZSetDriver<C>
where
    C: ConnectionLike + Send + Sync + Clone + 'static,
{
    type Error = Error;

    fn node_id(&self) -> String {
        self.node_id.clone()
    }

    /// Get the list of nodes from the redis server, use `ZRANGEBYSCORE` to get the latest nodes
    async fn get_nodes(&self) -> Result<Vec<String>, Self::Error> {
        let key = utils::get_zset_key(&self.service_name);

        let mut con = self.con.clone();
        let min = (chrono::Utc::now() - chrono::Duration::seconds(self.timeout as i64)).timestamp(); // current timestamp - timeout
        let nodes: Vec<String> = con.zrangebyscore(key, min, "+inf").await?;
        Ok(nodes)
    }

    /// Start a routine to send the heartbeat to the redis server
    async fn start(&mut self) -> Result<(), Self::Error> {
        // check if the driver has already started
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // start the heartbeat
        let mut con = self.con.clone();
        let service_name = self.service_name.clone();
        let node_id = self.node_id.clone();
        let started = self.started.clone();
        let shutdown_notify = self.notify.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

            while started.load(Ordering::SeqCst) {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = register_node(&service_name, &node_id, &mut con, chrono::Utc::now().timestamp()).await {
                            tracing::error!("Failed to register node: {:?}", e);
                        }
                    }
                    _ = shutdown_notify.notified() => {
                        tracing::info!("Heartbeat task received shutdown signal");
                        break;
                    }
                }
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
        self.shutdown();
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
async fn register_node<C: ConnectionLike + Send + Sync>(
    service_name: &str,
    node_id: &str,
    con: &mut C,
    time: i64,
) -> Result<(), redis::RedisError> {
    con.zadd(utils::get_zset_key(service_name), node_id, time).await?;

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

        let mut mock_con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("ZADD")
                .arg(utils::get_zset_key(service_name))
                .arg(ts)
                .arg(node_id),
            Ok(redis::Value::Okay),
        )]);

        // Perform the node registration
        let result = register_node(service_name, node_id, &mut mock_con, ts).await;

        assert!(
            result.is_ok(),
            "Register node should be successful: {}",
            result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_get_nodes_success() {
        let service_name = "test-service";
        let node_id = "test-node";

        let keys = ["node1", "node2", "node3"];
        let keys_as_redis_value: Vec<redis::Value> = keys
            .iter()
            .map(|k| redis::Value::BulkString(k.as_bytes().to_vec()))
            .collect();

        let mock_con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("ZRANGEBYSCORE")
                .arg(utils::get_zset_key(service_name))
                .arg((chrono::Utc::now() - chrono::Duration::seconds(DEFAULT_TIMEOUT as i64)).timestamp())
                .arg("+inf"),
            Ok(redis::Value::Array(keys_as_redis_value)),
        )]);

        // Perform the node registration
        let driver = RedisZSetDriver::new(mock_con, service_name, node_id).await.unwrap();
        let result = driver.get_nodes().await;

        assert!(
            result.is_ok(),
            "Get nodes should be successful: {}",
            result.unwrap_err()
        );
        assert_eq!(result.unwrap(), keys, "The nodes should match");
    }
}
