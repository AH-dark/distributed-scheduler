use std::fmt::Debug;

use super::Driver;

const LOCAL_NODE_ID: &str = "local";

#[derive(Clone, Debug)]
pub struct LocalDriver;

#[async_trait::async_trait]
impl Driver for LocalDriver {
    fn node_id(&self) -> String {
        LOCAL_NODE_ID.to_string()
    }

    /// Scan the redis server to get the nodes
    async fn get_nodes(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(vec![LOCAL_NODE_ID.to_string()])
    }

    /// Start a routine to send heartbeat to the redis server
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
