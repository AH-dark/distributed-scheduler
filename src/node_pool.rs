use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8};

use thiserror::Error;
use tokio::sync::RwLock;

use crate::driver::Driver;

pub struct NodePool {
    service_name: String,
    node_id: String,

    pre_nodes: Arc<RwLock<Vec<String>>>,
    hash: Arc<RwLock<hashring::HashRing<String>>>,
    driver: Arc<dyn Driver>,

    state_lock: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("No node available")]
    NoNodeAvailable,
    #[error("Driver error: {0}")]
    DriverError(#[from] Box<dyn std::error::Error>),
}

impl NodePool {
    pub fn new(service_name: String, driver: Arc<dyn Driver>) -> Self {
        Self {
            service_name,
            node_id: String::new(),
            pre_nodes: Arc::new(RwLock::new(Vec::new())),
            hash: Arc::new(RwLock::new(hashring::HashRing::new())),
            driver,
            state_lock: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn update_hash_ring(&self, nodes: Vec<String>) -> Result<(), Error> {
        update_hash_ring(
            self.pre_nodes.clone(),
            self.state_lock.clone(),
            self.hash.clone(),
            nodes,
        ).await
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.driver.start().await?;
        self.node_id = self.driver.node_id();
        self.update_hash_ring(self.driver.get_nodes().await?).await?;

        tokio::spawn({
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            let pre_nodes = self.pre_nodes.clone();
            let state_lock = self.state_lock.clone();
            let hash = self.hash.clone();
            let driver = self.driver.clone();
            let stop = self.stop.clone();

            let error_time = AtomicU8::new(0);

            async move {
                loop {
                    interval.tick().await;
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }

                    let nodes = match driver.get_nodes().await {
                        Ok(nodes) => nodes,
                        Err(_) => continue,
                    };

                    update_hash_ring(pre_nodes.clone(), state_lock.clone(), hash.clone(), nodes)
                        .await
                        .map_err(|e| {
                            log::error!("Failed to update hash ring: {:?}", e);
                            error_time.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        })
                        .ok();

                    if error_time.load(std::sync::atomic::Ordering::SeqCst) >= 5 {
                        panic!("Failed to update hash ring 5 times")
                    }
                }
            }
        });

        Ok(())
    }

    /// Check if the job should be executed on the current node.
    pub async fn check_job_available(&self, job_name: String) -> Result<bool, Error> {
        let hash = self.hash.read().await;
        match hash.get(&job_name) {
            Some(node) => Ok(node == &self.node_id),
            None => Err(Error::NoNodeAvailable),
        }
    }
}

async fn update_hash_ring(
    pre_nodes: Arc<RwLock<Vec<String>>>,
    state_lock: Arc<AtomicBool>,
    hash: Arc<RwLock<hashring::HashRing<String>>>,
    nodes: Vec<String>,
) -> Result<(), Error> {
    if equal_ring(&nodes, &pre_nodes.read().await) {
        return Ok(());
    }

    // Lock the state
    if state_lock.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        return Ok(());
    }

    // Update the pre_nodes
    pre_nodes.write().await.clone_from(&nodes);

    let mut hash = hash.write().await;
    *hash = hashring::HashRing::new();
    for node in nodes {
        hash.add(node);
    }

    // Unlock the state
    state_lock.store(false, std::sync::atomic::Ordering::SeqCst);

    Ok(())
}

fn equal_ring(a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut a_sorted = a.to_vec();
    let mut pre_nodes_sorted = b.to_vec();

    a_sorted.sort();
    pre_nodes_sorted.sort();

    a_sorted == pre_nodes_sorted
}
