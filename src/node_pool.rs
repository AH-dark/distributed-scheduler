use std::sync::{
    atomic::{AtomicBool, AtomicU8},
    Arc,
};

use thiserror::Error;
use tokio::sync::RwLock;

use crate::driver::Driver;

/// Node pool, used to manage nodes in a cluster.
pub struct NodePool<D>
where
    D: Driver + Send + Sync,
{
    node_id: String,

    pre_nodes: RwLock<Vec<String>>,
    hash: RwLock<hashring::HashRing<String>>,
    driver: Arc<D>,

    state_lock: AtomicBool,
    stop: AtomicBool,
}

impl<D> std::fmt::Debug for NodePool<D>
where
    D: Driver + Send + Sync + std::fmt::Debug,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("NodePool")
            .field("node_id", &self.node_id)
            .field("pre_nodes", &self.pre_nodes)
            .field("hash", &self.hash)
            .field("driver", &self.driver)
            .field("state_lock", &self.state_lock)
            .field("stop", &self.stop)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum Error<D>
where
    D: Driver + Send + Sync,
{
    #[error("No node available")]
    NoNodeAvailable,
    #[error("Driver error: {0}")]
    DriverError(D::Error),
}

impl<D> NodePool<D>
where
    D: Driver + Send + Sync,
{
    /// Create a new node pool with the given driver.
    pub async fn new(mut driver: D) -> Result<Self, Error<D>> {
        driver.start().await.map_err(Error::DriverError)?;

        // Update the hash ring
        let mut pre_nodes = Vec::new();
        let mut hash = hashring::HashRing::new();
        let state_lock = AtomicBool::new(false);

        update_hash_ring::<D>(
            &mut pre_nodes,
            &state_lock,
            &mut hash,
            &driver.get_nodes().await.map_err(Error::DriverError)?,
        )
        .await?;

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
    pub async fn start(self: Arc<Self>) -> Result<(), Error<D>> {
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

                match update_hash_ring::<D>(&mut pre_nodes, &self.state_lock, &mut hash, &nodes).await {
                    Ok(_) => {
                        error_time.store(0, std::sync::atomic::Ordering::SeqCst);
                    }
                    Err(_) => {
                        error_time.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        log::error!("Failed to update hash ring");
                    }
                }
            }

            if error_time.load(std::sync::atomic::Ordering::SeqCst) >= 5 {
                panic!("Failed to update hash ring 5 times")
            }
        }
    }

    /// Check if the job should be executed on the current node.
    pub(crate) async fn check_job_available(
        &self,
        job_name: &str,
    ) -> Result<bool, Error<D>> {
        let hash = self.hash.read().await;
        match hash.get(&job_name) {
            Some(node) if node == &self.node_id => Ok(true),
            Some(_) => Ok(false),
            None => Err(Error::NoNodeAvailable),
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl<D> Drop for NodePool<D>
where
    D: Driver + Send + Sync,
{
    fn drop(&mut self) {
        self.stop();
    }
}

/// Update the hash ring with the given nodes.
///
/// # Arguments
///
/// * `pre_nodes` - The previous nodes
/// * `state_lock` - The state lock
/// * `hash` - The hash ring
/// * `nodes` - The new nodes
async fn update_hash_ring<D>(
    pre_nodes: &mut Vec<String>,
    state_lock: &AtomicBool,
    hash: &mut hashring::HashRing<String>,
    nodes: &Vec<String>,
) -> Result<(), Error<D>>
where
    D: Driver + Send + Sync,
{
    if equal_ring(nodes, pre_nodes) {
        log::trace!("Nodes are equal, skipping update, nodes: {:?}", nodes);
        return Ok(());
    }

    log::info!(
        "Nodes detected, updating hash ring, pre_nodes: {:?}, now_nodes: {:?}",
        pre_nodes,
        nodes
    );

    // Lock the state
    if state_lock
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        )
        .is_err()
    {
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

/// Compare two rings.
fn equal_ring(
    a: &[String],
    b: &[String],
) -> bool {
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
    use crate::driver::local::LocalDriver;

    #[tokio::test]
    async fn test_update_hash_ring() {
        let mut pre_nodes = Vec::new();
        let state_lock = AtomicBool::new(false);
        let mut hash = hashring::HashRing::new();

        let nodes = vec!["node1".to_string(), "node2".to_string()];

        update_hash_ring::<LocalDriver>(&mut pre_nodes, &state_lock, &mut hash, &nodes)
            .await
            .unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node2".to_string()));

        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];

        update_hash_ring::<LocalDriver>(&mut pre_nodes, &state_lock, &mut hash, &nodes)
            .await
            .unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node2".to_string()));

        let nodes = vec!["node1".to_string(), "node3".to_string()];

        update_hash_ring::<LocalDriver>(&mut pre_nodes, &state_lock, &mut hash, &nodes)
            .await
            .unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node3".to_string()));

        let nodes = vec!["node1".to_string(), "node3".to_string()];

        update_hash_ring::<LocalDriver>(&mut pre_nodes, &state_lock, &mut hash, &nodes)
            .await
            .unwrap();
        assert_eq!(pre_nodes, nodes);
        assert_eq!(hash.get(&"test"), Some(&"node3".to_string()));
    }

    #[tokio::test]
    async fn test_equal_ring() {
        let a = vec!["node1".to_string(), "node2".to_string()];
        let b = vec!["node1".to_string(), "node2".to_string()];

        assert!(equal_ring(&a, &b));

        let a = vec!["node1".to_string(), "node2".to_string()];
        let b = vec!["node2".to_string(), "node1".to_string()];

        assert!(equal_ring(&a, &b));

        let a = vec!["node1".to_string(), "node2".to_string()];
        let b = vec!["node1".to_string(), "node3".to_string()];
        assert!(!equal_ring(&a, &b));
    }
}
