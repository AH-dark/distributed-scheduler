use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use etcd_client::*;
use tokio::sync::{Mutex, RwLock};

use super::{Driver, utils};

const ETCD_DEFAULT_LEASE_TTL: i64 = 5;

pub struct EtcdDriver {
    client: Arc<Mutex<Client>>,

    service_name: String,
    node_id: String,

    stop: Arc<AtomicBool>,
    node_list: Arc<RwLock<HashSet<String>>>,
}

impl std::fmt::Debug for EtcdDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f
            .debug_struct("EtcdDriver")
            .field("service_name", &self.service_name)
            .field("node_id", &self.node_id)
            .field("stop", &self.stop)
            .field("node_list", &self.node_list)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Etcd error: {0}")]
    Etcd(#[from] etcd_client::Error),
    #[error("Empty service name")]
    EmptyServiceName,
    #[error("Empty node id")]
    EmptyNodeId,
    #[error("Driver not started")]
    DriverNotStarted,
}

impl EtcdDriver {
    /// Create a new etcd driver with the given client, service name, and node id.
    pub async fn new(client: Client, service_name: &str, node_id: &str) -> Result<Self, Error> {
        if service_name.is_empty() {
            return Err(Error::EmptyServiceName);
        }

        if node_id.is_empty() {
            return Err(Error::EmptyNodeId);
        }

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            node_id: utils::get_key_prefix(service_name) + node_id,
            service_name: service_name.into(),
            stop: Arc::new(AtomicBool::new(true)),
            node_list: Arc::new(RwLock::new(HashSet::new())),
        })
    }
}

#[async_trait::async_trait]
impl Driver for EtcdDriver {
    fn node_id(&self) -> String {
        self.node_id.clone()
    }

    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Box::new(Error::DriverNotStarted));
        }

        Ok(self.node_list.read().await.iter().cloned().collect())
    }

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = self.client.lock().await;
        self.stop.store(false, std::sync::atomic::Ordering::SeqCst);

        // init node list
        let mut node_list = self.node_list.write().await;
        for kv in client.get(utils::get_key_prefix(&self.service_name), Some(GetOptions::new().with_prefix())).await?.kvs() {
            node_list.insert(kv.key_str()?.into());
        }

        // watch for node changes
        {
            let (mut watcher, mut watch_stream) = client.watch(utils::get_key_prefix(&self.service_name), Some(WatchOptions::new().with_prefix())).await?;
            let node_list = self.node_list.clone();
            let stop = self.stop.clone();
            tokio::spawn(async move {
                loop {
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        watcher.cancel().await.expect("Failed to cancel watcher");
                        break;
                    }

                    match watch_stream.message().await {
                        Ok(Some(resp)) => {
                            if resp.canceled() {
                                log::warn!("Watch stream canceled: {:?}", resp);
                                break;
                            }

                            for event in resp.events() {
                                let key = match event.kv() {
                                    Some(kv) if kv.key_str().is_ok() => kv.key_str().unwrap().to_string(),
                                    _ => continue,
                                };

                                match event.event_type() {
                                    EventType::Put => node_list.write().await.insert(key),
                                    EventType::Delete => node_list.write().await.remove(&key),
                                };
                            }
                        }
                        Ok(None) => panic!("Watch stream closed"),
                        Err(e) => panic!("Watch error: {:?}", e),
                    }
                }
            });
        }

        // register current node
        {
            log::info!("Registering node: {}", self.node_id);

            // grant a lease for the node key
            let lease = client.lease_grant(ETCD_DEFAULT_LEASE_TTL, None).await?;
            let lease_id = lease.id();

            // keep the lease alive
            let (mut keeper, mut ka_stream) = client.lease_keep_alive(lease.id()).await?;
            let stop = self.stop.clone();
            let inner_client = self.client.clone();

            // spawn a task to keep the lease alive
            tokio::spawn(async move {
                keeper.keep_alive().await.expect("Failed to keep alive");

                loop {
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        inner_client.lock().await.lease_revoke(lease_id).await.expect("Failed to revoke lease");
                        break;
                    }

                    match ka_stream.message().await {
                        Ok(Some(_)) => keeper.keep_alive().await.expect("Failed to keep alive"),
                        Ok(None) => panic!("Keep alive stream closed"),
                        Err(e) => panic!("Keep alive error: {:?}", e),
                    }
                }
            });

            // put the node key
            client.put(self.node_id.as_str(), self.node_id.as_str(), Some(PutOptions::new().with_lease(lease_id))).await?;
        }

        Ok(())
    }
}

impl Drop for EtcdDriver {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
