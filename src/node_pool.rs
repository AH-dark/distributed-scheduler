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
    pub fn new(driver: Arc<dyn Driver>) -> Self {
        Self {
            node_id: String::new(),
            pre_nodes: RwLock::new(Vec::new()),
            hash: RwLock::new(hashring::HashRing::new()),
            driver,
            state_lock: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        }
    }

    pub async fn init(&mut self) -> Result<(), Error> {
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

pub async fn start(node_pool: Arc<NodePool>) -> Result<(), Error> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let error_time = AtomicU8::new(0);

        loop {
            interval.tick().await;
            if node_pool.stop.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }

            // independent ownership
            {
                let nodes = match node_pool.driver.get_nodes().await {
                    Ok(nodes) => nodes,
                    Err(_) => continue,
                };

                let mut pre_nodes = node_pool.pre_nodes.write().await;
                let mut hash = node_pool.hash.write().await;

                update_hash_ring(&mut pre_nodes, &node_pool.state_lock, &mut hash, &nodes)
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
    });

    Ok(())
}

/// Check if the job should be executed on the current node.
pub async fn check_job_available(node_pool: &NodePool, job_name: &str) -> Result<bool, Error> {
    let hash = node_pool.hash.read().await;
    match hash.get(&job_name) {
        Some(node) => Ok(node == &node_pool.node_id),
        None => Err(Error::NoNodeAvailable),
    }
}

async fn update_hash_ring(
    pre_nodes: &mut Vec<String>,
    state_lock: &AtomicBool,
    hash: &mut hashring::HashRing<String>,
    nodes: &Vec<String>,
) -> Result<(), Error> {
    if equal_ring(&nodes, pre_nodes) {
        return Ok(());
    }

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
