use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8};

use thiserror::Error;
use tokio::sync::RwLock;

use crate::driver::Driver;

#[derive(Debug)]
pub struct NodePool {
    node_id: String,

    pre_nodes: RwLock<Vec<String>>,
    hash: RwLock<hashring::HashRing<String>>,
    driver: Arc<dyn Driver>,

    state_lock: AtomicBool,
    stop: AtomicBool,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("No node available")]
    NoNodeAvailable,
    #[error("Driver error: {0}")]
    DriverError(#[from] Box<dyn std::error::Error>),
}

impl NodePool {
    /// Create a new node pool with the given driver.
    pub fn new<T: Driver + 'static>(driver: T) -> Self {
        Self {
            node_id: String::new(),
            pre_nodes: RwLock::new(Vec::new()),
            hash: RwLock::new(hashring::HashRing::new()),
            driver: Arc::new(driver),
            state_lock: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        }
    }

    /// Start the node pool, blocking the current thread.
    pub async fn start(self: Arc<Self>) -> Result<(), Error> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let error_time = AtomicU8::new(0);

        loop {
            interval.tick().await;
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            // independent ownership
            {
                let nodes = match self.driver.get_nodes().await {
                    Ok(nodes) => nodes,
                    Err(_) => continue,
                };

                let mut pre_nodes = self.pre_nodes.write().await;
                let mut hash = self.hash.write().await;

                update_hash_ring(&mut pre_nodes, &self.state_lock, &mut hash, &nodes)
                    .await
                    .map_err(|e| {
                        log::error!("Failed to update hash ring: {:?}", e);
                        error_time.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    })
                    .ok();
            }

            if error_time.load(std::sync::atomic::Ordering::SeqCst) >= 5 {
                panic!("Failed to update hash ring 5 times")
            }
        }
    }

    /// Initialize the node pool.
    pub(crate) async fn init(&mut self) -> Result<(), Error> {
        self.driver.start().await?;
        self.node_id = self.driver.node_id();

        // Update the hash ring
        let mut pre_nodes = self.pre_nodes.write().await;
        let mut hash = self.hash.write().await;

        update_hash_ring(&mut pre_nodes, &self.state_lock, &mut hash, &self.driver.get_nodes().await?).await?;

        drop(pre_nodes);
        drop(hash);

        Ok(())
    }

    /// Check if the job should be executed on the current node.
    pub(crate) async fn check_job_available(&self, job_name: &str) -> Result<bool, Error> {
        let hash = self.hash.read().await;
        match hash.get(&job_name) {
            Some(node) if node == &self.node_id => Ok(true),
            Some(_) => Ok(false),
            None => Err(Error::NoNodeAvailable),
        }
    }

    pub async fn stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for NodePool {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.stop();
    }
}

async fn update_hash_ring(
    pre_nodes: &mut Vec<String>,
    state_lock: &AtomicBool,
    hash: &mut hashring::HashRing<String>,
    nodes: &Vec<String>,
) -> Result<(), Error> {
    if equal_ring(nodes, pre_nodes) {
        return Ok(());
    }

    log::info!("Nodes detected, updating hash ring, pre_nodes: {:?}, now_nodes: {:?}", pre_nodes, nodes);

    // Lock the state
    if state_lock.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        return Ok(());
    }

    // Update the pre_nodes
    pre_nodes.clone_from(nodes);

    *hash = hashring::HashRing::new();
    for node in nodes {
        hash.add(node.clone());
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
