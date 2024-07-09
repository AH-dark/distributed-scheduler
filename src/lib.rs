/// This is the main library file for the project.

/// The `cron` module contains the `Cron` struct, which is the main entry point for the library, providing the ability to add and remove jobs.
pub mod cron;

/// The `driver` module contains the `Driver` trait and its implementations.
pub mod driver;

/// The `node_pool` module contains the `NodePool` struct, which is used to manage the nodes in the cluster.
pub mod node_pool;
