pub fn get_node_id(service_name: &str) -> String {
    format!("{}:{}", service_name, uuid::Uuid::new_v4())
}

pub static GLOBAL_KEY_PREFIX: &str = "distributed-scheduler";

pub fn get_key_prefix(service_name: &str) -> String {
    format!("{}:{}:", GLOBAL_KEY_PREFIX, service_name)
}
