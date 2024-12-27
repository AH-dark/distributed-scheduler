use std::{collections::HashMap, future::Future, sync::Arc};

use job_scheduler::{Schedule, Uuid};
use tokio::sync::Mutex;

use crate::{driver::Driver, node_pool};

/// The `Cron` struct is the main entry point for the library, providing the ability to add and
/// remove jobs.
pub struct Cron<'a, D>
where
    D: Driver + Send + Sync,
{
    node_pool: Arc<node_pool::NodePool<D>>,
    jobs: Mutex<HashMap<String, Uuid>>,
    scheduler: Arc<Mutex<job_scheduler::JobScheduler<'a>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node pool error: {0}")]
    NodePool(#[from] node_pool::Error),
}

/// Run the scheduler in a separate task, return a Future
async fn run_scheduler<'a>(job_scheduler: Arc<Mutex<job_scheduler::JobScheduler<'a>>>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    loop {
        job_scheduler.lock().await.tick();
        interval.tick().await;
    }
}

impl<'a, D> Cron<'a, D>
where
    D: Driver + Send + Sync + 'static,
{
    /// Create a new cron with the given node pool.
    pub async fn new(np: node_pool::NodePool<D>) -> Self {
        Self {
            node_pool: Arc::new(np),
            jobs: Mutex::new(HashMap::new()),
            scheduler: Arc::new(Mutex::new(job_scheduler::JobScheduler::new())),
        }
    }

    /// Start the cron, blocking the current thread.
    pub async fn start(&self) {
        let np = self.node_pool.clone();

        tokio::select! {
            _ = run_scheduler(self.scheduler.clone()) => {},
            Err(err) = np.start() => {
                panic!("Failed to start node pool: {}", err);
            },
        }
    }

    /// Register a job in the scheduler
    async fn register_job(
        &self,
        job_name: &str,
        job: job_scheduler::Job<'a>,
    ) {
        let mut cron = self.scheduler.lock().await;
        let id = cron.add(job);
        self.jobs.lock().await.insert(job_name.to_string(), id);
    }

    /// Add a job to the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    /// * `schedule` - The schedule of the job
    /// * `run` - The function to run
    pub async fn add_job<F>(
        &self,
        job_name: &str,
        schedule: Schedule,
        run: F,
    ) -> Result<(), Error>
    where
        F: 'static + Sync + Send + Fn(),
    {
        let run = Arc::new(run);

        let job = job_scheduler::Job::new(schedule, {
            let job_name = job_name.to_string();
            let np = self.node_pool.clone();

            move || {
                let job_name = job_name.clone();
                let np = np.clone();
                let run = run.clone();

                tokio::spawn(async move {
                    match np.check_job_available(&job_name).await {
                        Ok(is_this_node) if is_this_node => run(),
                        Ok(_) => {
                            log::trace!("Job is not available on this node")
                        }
                        Err(e) => log::error!("Failed to check job availability: {}", e),
                    }
                });
            }
        });

        self.register_job(job_name, job).await;

        Ok(())
    }

    /// Add an async job to the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    /// * `schedule` - The schedule of the job
    /// * `run` - The async function to run
    pub async fn add_async_job<F, Fut>(
        &self,
        job_name: &str,
        schedule: Schedule,
        run: F,
    ) -> Result<(), Error>
    where
        F: 'static + Sync + Send + Fn() -> Fut,
        Fut: Future<Output = ()> + Send,
    {
        let run = Arc::new(run);

        let job = job_scheduler::Job::new(schedule, {
            let job_name = job_name.to_string();
            let np = Arc::clone(&self.node_pool);
            let run = Arc::clone(&run);

            move || {
                let job_name = job_name.clone();
                let np = Arc::clone(&np);
                let run = Arc::clone(&run);

                // spawn the async job
                tokio::spawn(async move {
                    // check if the job is available
                    if np
                        .check_job_available(&job_name)
                        .await
                        .is_ok_and(|is_this_node| is_this_node)
                    {
                        run().await;
                    }
                });
            }
        });

        self.register_job(job_name, job).await;

        Ok(())
    }

    /// Remove a job from the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    pub async fn remove_job(
        &self,
        job_name: &str,
    ) -> Result<(), Error> {
        if let Some(id) = self.jobs.lock().await.remove(job_name) {
            self.scheduler.lock().await.remove(id);
        }

        Ok(())
    }
}

impl<D> Drop for Cron<'_, D>
where
    D: Driver + Send + Sync,
{
    fn drop(&mut self) {
        self.node_pool.stop();
    }
}
