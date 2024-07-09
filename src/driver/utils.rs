pub static GLOBAL_KEY_PREFIX: &str = "distributed-scheduler";

/// Get the key prefix for the given service name.
pub fn get_key_prefix(service_name: &str) -> String {
    format!("{}:{}:", GLOBAL_KEY_PREFIX, service_name)
}

#[cfg(feature = "driver-redis")]
/// Get the zset key for the given service name.
pub fn get_zset_key(service_name: &str) -> String {
    format!("{}:{}", GLOBAL_KEY_PREFIX, service_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_key_prefix() {
        assert_eq!(get_key_prefix("test"), "distributed-scheduler:test:");
    }

    #[test]
    #[cfg(feature = "driver-redis")]
    fn test_get_zset_key() {
        assert_eq!(get_zset_key("test"), "distributed-scheduler:test");
    }
}
