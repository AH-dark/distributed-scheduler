# Distributed Scheduler

This is a distributed scheduler that can be used to schedule tasks on a cluster of machines. The scheduler is
implemented in Rust and uses the tokio library for asynchronous IO. The scheduler is designed to be fault-tolerant
and can recover from failures of the scheduler itself or the machines in the cluster.

The current cronjob provider is [job_scheduler](https://github.com/lholden/job_scheduler), but it is no longer
maintained, so I may replace it with another crate in the future.

## Architecture

The scheduler is composed of a number of components that work together to schedule tasks. The main components are:

- **Cron**: The main scheduler that schedules tasks on the cluster. It is responsible for maintaining the state of the
  cluster and scheduling tasks on the machines in the cluster.
- **Node Pool**: The node pool is responsible for managing the nodes in single machine. It will check if the job should
  be run on the local machine or not. The scheduling algorithm is based on consistent hashing.
- **Driver**: We provide multiple drivers to run the job. The driver is responsible for maintain the job state and sync
  the node list.

## Available Drivers

- [x] Redis (default)
- [x] Redis (Z-SET)
- [ ] Etcd

## Usage

To use the scheduler, you can view the example crate in the `examples` directory. Here is an brief example:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rdb = redis::Client::open("redis://localhost:6379").unwrap();

    let driver = distributed_schedular::driver::redis::RedisDriver::new(rdb).await?;
    let np = distributed_schedular::node_pool::NodePool::new(driver);
    let cron = distributed_schedular::cron::Cron::new(np).await?;

    cron.add_job("test", "1/10 * * * * *".parse().unwrap(), || {
        println!("Hello, I run every 10 seconds");
    }).await?;

    Ok(())
}
```

## License

This project is licensed under the AGPL-3.0 License - see the [LICENSE](LICENSE) file for details.
