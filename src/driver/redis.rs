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

impl RedisDriver {
    pub async fn new(client: redis::Client) -> Result<Self, redis::RedisError> {
        Ok(Self {
            con: client.get_multiplexed_async_connection().await?,
            service_name: String::new(),
            node_id: String::new(),
            started: Arc::new(AtomicBool::new(false)),
            timeout: 5,
        })
    }
}


#[async_trait::async_trait]
impl Driver for RedisDriver {
    async fn init(&mut self, service_name: String) -> Result<(), Box<dyn std::error::Error>> {
        self.service_name.clone_from(&service_name);
        self.node_id = utils::get_node_id(&service_name);

        Ok(())
    }

    async fn node_id(&self) -> Result<&str, Box<dyn std::error::Error>> {
        Ok(&self.node_id)
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
            let node_id = self.node_id.clone();
            let timeout = self.timeout;
            let started = self.started.clone();

            async move {
                heartbeat(node_id, timeout, con, started)
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

async fn register_node(node_id: &String, timeout: u64, con: &mut redis::aio::MultiplexedConnection) -> Result<(), Box<dyn std::error::Error>> {
    con.set_ex(node_id, node_id, timeout).await?;
    Ok(())
}

async fn heartbeat(node_id: String, timeout: u64, con: redis::aio::MultiplexedConnection, started: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
    let mut con = con;

    log::debug!("Started heartbeat");

    loop {
        // check if the driver has stopped
        if !started.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }

        interval.tick().await;

        register_node(&node_id, timeout, &mut con)
            .await
            .map_err(|e| {
                log::error!("Failed to register node: {:?}", e);
            })
            .ok();
    }

    log::debug!("Stopped heartbeat");
    Ok(())
}
