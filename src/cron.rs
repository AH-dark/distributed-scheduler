use std::collections::HashMap;
use std::sync::Arc;

use job_scheduler::{Schedule, Uuid};
use tokio::sync::Mutex;

use crate::node_pool;

pub struct Cron<'a> {
    node_pool: Arc<node_pool::NodePool>,
    jobs: Mutex<HashMap<String, Uuid>>,
    cron: Mutex<job_scheduler::JobScheduler<'a>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node pool error: {0}")]
    NodePool(#[from] node_pool::Error),
}

impl<'a> Cron<'a> {
    pub async fn new(mut np: node_pool::NodePool) -> Result<Self, node_pool::Error> {
        np.init().await?;

        let np = Arc::new(np);
        node_pool::start(np.clone()).await?;

        Ok(Self {
            node_pool: np,
            jobs: Mutex::new(HashMap::new()),
            cron: Mutex::new(job_scheduler::JobScheduler::new()),
        })
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

        let mut cron = self.cron.lock().await;
        let id = cron.add(job_scheduler::Job::new(schedule, {
            let job_name = job_name.to_string();
            let np = self.node_pool.clone();

            move || {
                let job_name = job_name.clone();
                let np = np.clone();
                let run = run.clone();

                tokio::task::spawn_blocking(|| async move {
                    // check if the job is available
                    if np.check_job_available(&job_name).await.is_ok_and(|b| b) {
                        run();
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

        let mut cron = self.cron.lock().await;
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
            let mut cron = self.cron.lock().await;
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
