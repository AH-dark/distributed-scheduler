use std::collections::HashMap;
use std::sync::Arc;

use job_scheduler::{Schedule, Uuid};
use tokio::sync::{Mutex, RwLock};

use crate::node_pool;

pub struct Cron<'a> {
    node_pool: Arc<node_pool::NodePool>,
    jobs: Mutex<HashMap<String, Uuid>>,
    cron: Arc<RwLock<job_scheduler::JobScheduler<'a>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node pool error: {0}")]
    NodePool(#[from] node_pool::Error),
}

async fn run_cron<'a>(job_scheduler: Arc<RwLock<job_scheduler::JobScheduler<'a>>>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    loop {
        job_scheduler.write().await.tick();
        interval.tick().await;
    }
}

impl<'a> Cron<'a> {
    /// Create a new cron with the given node pool.
    pub async fn new(np: node_pool::NodePool) -> Self {
        Self {
            node_pool: Arc::new(np),
            jobs: Mutex::new(HashMap::new()),
            cron: Arc::new(RwLock::new(job_scheduler::JobScheduler::new())),
        }
    }

    /// Start the cron, blocking the current thread.
    pub async fn start(&self) {
        let np = self.node_pool.clone();

        tokio::select! {
            _ = run_cron(self.cron.clone()) => {},
            val = np.start() => {
                val.expect("Failed to start node pool");
            },
        }
    }

    /// Add a job to the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    /// * `schedule` - The schedule of the job
    /// * `run` - The function to run
    ///
    pub async fn add_job<T>(&self, job_name: &str, schedule: Schedule, run: T) -> Result<(), Error> where T: 'static + Sync + Send + Fn() {
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
                        Ok(_) => { log::trace!("Job is not available on this node") }
                        Err(e) => log::error!("Failed to check job availability: {}", e),
                    }
                });
            }
        });
        let id = self.cron.write().await.add(job);

        let mut jobs = self.jobs.lock().await;
        jobs.insert(job_name.to_string(), id);

        Ok(())
    }

    /// Add an async job to the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    /// * `schedule` - The schedule of the job
    /// * `run` - The async function to run
    ///
    pub async fn add_async_job<T>(&self, job_name: &str, schedule: Schedule, run: T) -> Result<(), Error> where T: 'static + Sync + Send + Fn() -> tokio::task::JoinHandle<()> {
        let run = Arc::new(run);

        let mut cron = self.cron.write().await;
        let id = cron.add(job_scheduler::Job::new(schedule, {
            let job_name = job_name.to_string();
            let np = self.node_pool.clone();

            move || {
                let job_name = job_name.clone();
                let np = np.clone();
                let run = run.clone();

                // spawn the async job
                tokio::task::spawn_blocking(|| async move {
                    // check if the job is available
                    if np.check_job_available(&job_name)
                        .await
                        .is_ok_and(|is_this_node| is_this_node) {
                        run().await.expect("Failed to run async job, runtime error");
                    }
                });
            }
        }));
        drop(cron);

        let mut jobs = self.jobs.lock().await;
        jobs.insert(job_name.to_string(), id);
        drop(jobs);

        Ok(())
    }

    /// Remove a job from the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    ///
    pub async fn remove_job(&self, job_name: &str) -> Result<(), Error> {
        let mut jobs = self.jobs.lock().await;
        if let Some(id) = jobs.remove(job_name) {
            let mut cron = self.cron.write().await;
            cron.remove(id);
        }
        drop(jobs);

        Ok(())
    }
}

impl<'a> Drop for Cron<'a> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.node_pool.stop();
    }
}
