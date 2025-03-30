use std::{convert::Infallible, fmt::Debug};

use super::Driver;

const LOCAL_NODE_ID: &str = "local";

#[derive(Clone, Debug)]
pub struct LocalDriver;

#[async_trait::async_trait]
impl Driver for LocalDriver {
    type Error = Infallible;

    fn node_id(&self) -> String {
        LOCAL_NODE_ID.to_string()
    }

    /// Scan the redis server to get the nodes
    async fn get_nodes(&self) -> Result<Vec<String>, Self::Error> {
        Ok(vec![LOCAL_NODE_ID.to_string()])
    }
}
