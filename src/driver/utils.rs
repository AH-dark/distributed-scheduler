pub static GLOBAL_KEY_PREFIX: &str = "distributed-scheduler";

pub fn get_key_prefix(service_name: &str) -> String {
    format!("{}:{}:", GLOBAL_KEY_PREFIX, service_name)
}
