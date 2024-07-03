use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use redis::AsyncCommands;

use super::{Driver, utils};

#[derive(Debug)]
pub struct RedisDriver {
    con: redis::aio::MultiplexedConnection,

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

impl RedisDriver {
    pub async fn new(client: redis::Client, service_name: &str, node_id: &str) -> Result<Self, Error> {
        if service_name.is_empty() {
            return Err(Error::EmptyServiceName);
        }

        if node_id.is_empty() {
            return Err(Error::EmptyNodeId);
        }

        Ok(Self {
            con: client.get_multiplexed_async_connection().await?,
            service_name: service_name.into(),
            node_id: node_id.into(),
            started: Arc::new(AtomicBool::new(false)),
            timeout: 5,
        })
    }
}


#[async_trait::async_trait]
impl Driver for RedisDriver {
    fn node_id(&self) -> String {
        utils::get_key_prefix(&self.service_name) + &self.node_id
    }

    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let pattern = utils::get_key_prefix(&self.service_name) + "*";

        let mut con = self.con.clone();
        let mut res = con.scan_match(pattern).await?;

        let mut nodes: Vec<String> = Vec::new();
        while let Some(key) = res.next_item().await {
            nodes.push(key);
        }
        Ok(nodes)
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
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
            let timeout = self.timeout;
            let started = self.started.clone();

            async move {
                heartbeat(&service_name, &node_id, timeout, con, started)
                    .await
                    .expect("Failed to start scheduler driver heartbeat")
            }
        });

        Ok(())
    }
}

impl Drop for RedisDriver {
    fn drop(&mut self) {
        self.started.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Register the node in the redis
///
async fn register_node(combined_id: &str, timeout: u64, con: &mut redis::aio::MultiplexedConnection) -> Result<(), Box<dyn std::error::Error>> {
    con.set_ex(combined_id, combined_id, timeout).await?;
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
async fn heartbeat(service_name: &str, node_id: &str, timeout: u64, con: redis::aio::MultiplexedConnection, started: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let mut con = con;
    let combined_id = &(utils::get_key_prefix(service_name) + node_id);
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
        register_node(combined_id, timeout, &mut con).await
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
