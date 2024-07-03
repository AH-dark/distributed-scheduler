pub static GLOBAL_KEY_PREFIX: &str = "distributed-scheduler";

pub fn get_key_prefix(service_name: &str) -> String {
    format!("{}:{}:", GLOBAL_KEY_PREFIX, service_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_key_prefix() {
        assert_eq!(get_key_prefix("test"), "distributed-scheduler:test:");
    }
}
