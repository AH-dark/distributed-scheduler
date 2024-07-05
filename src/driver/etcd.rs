use std::collections::HashSet;
use std::sync::Arc;

use etcd_client::*;
use tokio::sync::{Mutex, RwLock};

use super::{Driver, utils};

const ETCD_DEFAULT_LEASE_TTL: i64 = 5;

pub struct EtcdDriver {
    client: Arc<Mutex<Client>>,

    service_name: String,
    node_id: String,

    watcher: Option<Watcher>,
    lease_keeper: Option<LeaseKeeper>,
    node_list: Arc<RwLock<HashSet<String>>>,
}

impl std::fmt::Debug for EtcdDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f
            .debug_struct("EtcdDriver")
            .field("service_name", &self.service_name)
            .field("node_id", &self.node_id)
            .field("watcher", &self.watcher)
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
            watcher: None,
            lease_keeper: None,
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
        if self.lease_keeper.is_none() || self.lease_keeper.is_none() {
            return Err(Error::DriverNotStarted.into());
        }

        let node_list = self.node_list.read().await;
        let nodes = node_list.iter().cloned().collect();
        Ok(nodes)
    }

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = self.client.lock().await;

        // init node list
        let mut node_list = self.node_list.write().await;
        for kv in client.get(utils::get_key_prefix(&self.service_name), Some(GetOptions::new().with_prefix())).await?.kvs() {
            node_list.insert(kv.value_str()?.into());
        }

        // watch for node changes
        {
            let (watcher, mut watch_stream) = client.watch(utils::get_key_prefix(&self.service_name), Some(WatchOptions::new().with_prefix())).await?;
            self.watcher = Some(watcher);
            let node_list = self.node_list.clone();
            tokio::spawn(async move {
                loop {
                    match watch_stream.message().await {
                        Ok(Some(resp)) => {
                            for event in resp.events() {
                                let key = match event.kv() {
                                    Some(kv) if kv.value_str().is_ok() => kv.value_str().unwrap(),
                                    _ => continue,
                                };

                                match event.event_type() {
                                    EventType::Put => node_list.write().await.insert(key.into()),
                                    EventType::Delete => node_list.write().await.remove(key),
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
            log::debug!("Lease granted: {:?}", lease);

            // keep the lease alive
            let (keeper, mut ka_stream) = client.lease_keep_alive(lease.id()).await?;
            self.lease_keeper = Some(keeper);

            // spawn a task to keep the lease alive
            tokio::spawn(async move {
                log::info!("Starting keep alive");

                loop {
                    match ka_stream.message().await {
                        Ok(Some(r)) => log::debug!("Keep alive response: {:?}", r),
                        Ok(None) => panic!("Keep alive stream closed"),
                        Err(e) => panic!("Keep alive error: {:?}", e),
                    }
                }
            });

            if let Some(keeper) = self.lease_keeper.as_mut() {
                keeper.keep_alive().await?;
            }

            // put the node key
            let res = client.put(self.node_id.as_str(), self.node_id.as_str(), Some(PutOptions::new().with_lease(lease.id()))).await?;
            log::debug!("Put result: {:?}", res);
        }

        Ok(())
    }
}

impl Drop for EtcdDriver {
    fn drop(&mut self) {
        if let Some(mut watcher) = self.watcher.take() {
            tokio::spawn(async move {
                watcher.cancel().await.expect("Failed to cancel watcher");
            });
        }

        if let Some(keeper) = self.lease_keeper.take() {
            let client = self.client.clone();
            tokio::spawn(async move {
                client.lock().await.lease_revoke(keeper.id()).await.expect("Failed to revoke lease");
            });
        }
    }
}
