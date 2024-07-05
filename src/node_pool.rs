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
    pub async fn new<T: Driver + 'static>(mut driver: T) -> Result<Self, Error> {
        driver.start().await?;

        // Update the hash ring
        let mut pre_nodes = Vec::new();
        let mut hash = hashring::HashRing::new();
        let state_lock = AtomicBool::new(false);

        update_hash_ring(&mut pre_nodes, &state_lock, &mut hash, &driver.get_nodes().await?).await?;

        Ok(Self {
            node_id: driver.node_id(),
            pre_nodes: RwLock::new(pre_nodes),
            hash: RwLock::new(hash),
            driver: Arc::new(driver),
            state_lock,
            stop: AtomicBool::new(false),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_update_hash_ring() {
        let mut pre_nodes = Vec::new();
        let state_lock = AtomicBool::new(false);
        let mut hash = hashring::HashRing::new();

        let nodes = vec!["node1".to_string(), "node2".to_string()];

        update_hash_ring(&mut pre_nodes, &state_lock, &mut hash, &nodes).await.unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node2".to_string()));

        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];

        update_hash_ring(&mut pre_nodes, &state_lock, &mut hash, &nodes).await.unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node2".to_string()));

        let nodes = vec!["node1".to_string(), "node3".to_string()];

        update_hash_ring(&mut pre_nodes, &state_lock, &mut hash, &nodes).await.unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node3".to_string()));

        let nodes = vec!["node1".to_string(), "node3".to_string()];

        update_hash_ring(&mut pre_nodes, &state_lock, &mut hash, &nodes).await.unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node3".to_string()));
    }

    #[tokio::test]
    async fn test_equal_ring() {
        let a = vec!["node1".to_string(), "node2".to_string()];
        let b = vec!["node1".to_string(), "node2".to_string()];

        assert_eq!(equal_ring(&a, &b), true);

        let a = vec!["node1".to_string(), "node2".to_string()];
        let b = vec!["node2".to_string(), "node1".to_string()];

        assert_eq!(equal_ring(&a, &b), true);

        let a = vec!["node1".to_string(), "node2".to_string()];
        let b = vec!["node1".to_string(), "node3".to_string()];
        assert_eq!(equal_ring(&a, &b), false);
    }
}
