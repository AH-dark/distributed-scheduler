use std::{collections::HashMap, future::Future, sync::Arc};

use job_scheduler_ng::{Schedule, Uuid};
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::{driver::Driver, node_pool};

/// The `Cron` struct is the main entry point for the library, providing the ability to add and
/// remove jobs.
pub struct Cron<'a, D>
where
    D: Driver + Send + Sync,
{
    node_pool: Arc<node_pool::NodePool<D>>,
    jobs: Mutex<HashMap<String, Uuid>>,
    scheduler: Arc<Mutex<job_scheduler_ng::JobScheduler<'a>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error<D>
where
    D: Driver + Send + Sync + 'static,
{
    #[error("schedule stopped")]
    SchedulerStopped,
    #[error("job name already exists")]
    JobNameConflict,
    #[error("node pool error: {0}")]
    NodePool(node_pool::Error<D>),
}

/// Run the scheduler in a separate task, return a Future
async fn run_scheduler(job_scheduler_ng: Arc<Mutex<job_scheduler_ng::JobScheduler<'_>>>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    loop {
        job_scheduler_ng.lock().await.tick();
        interval.tick().await;
    }
}

impl<'a, D> Cron<'a, D>
where
    D: Driver + Send + Sync + 'static,
{
    /// Create a new cron with the given node pool.
    pub async fn new(np: node_pool::NodePool<D>) -> Arc<Self> {
        Arc::new(Self {
            node_pool: Arc::new(np),
            jobs: Mutex::new(HashMap::new()),
            scheduler: Arc::new(Mutex::new(job_scheduler_ng::JobScheduler::new())),
        })
    }

    /// Start the cron, blocking the current thread.
    pub async fn start(&self) -> Result<(), Error<D>> {
        let np = self.node_pool.as_ref();

        tokio::select! {
            _ = run_scheduler(self.scheduler.clone()) => {
                Err(Error::SchedulerStopped)
            },
            Err(err) = np.start() => {
                Err(Error::NodePool(err))
            },
        }
    }

    /// Register a job in the scheduler
    #[tracing::instrument(skip(self, job), err)]
    async fn register_job(
        &self,
        job_name: &str,
        job: job_scheduler_ng::Job<'a>,
    ) -> Result<(), Error<D>> {
        // check if job name is conflict
        if self.jobs.lock().await.contains_key(job_name) {
            return Err(Error::JobNameConflict);
        }

        let mut cron = self.scheduler.lock().await;
        let id = cron.add(job);
        self.jobs.lock().await.insert(job_name.to_string(), id);

        tracing::info!(
            name: "job_registered",
            message = format!("Job {} registered", job_name),
            scheduler.uuid = id.to_string(),
            job.name = job_name
        );

        Ok(())
    }

    /// Add a job to the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    /// * `schedule` - The schedule of the job
    /// * `run` - The function to run
    #[tracing::instrument(skip(self, run), err)]
    pub async fn add_job<F>(
        &self,
        job_name: &str,
        schedule: Schedule,
        run: F,
    ) -> Result<(), Error<D>>
    where
        F: 'static + Sync + Send + Fn(),
    {
        let run = Arc::new(run);

        let job = job_scheduler_ng::Job::new(schedule, {
            let job_name = job_name.to_string();
            let np = self.node_pool.clone();

            move || {
                let job_name = job_name.clone();
                let np = np.clone();
                let run = run.clone();

                let job_name_trace = job_name.clone();

                tokio::spawn(
                    async move {
                        match np.check_job_available(&job_name).await {
                            Ok(is_this_node) if is_this_node => run(),
                            Ok(_) => {
                                tracing::trace!("Job is not available on this node")
                            }
                            Err(e) => tracing::error!("Failed to check job availability: {}", e),
                        }
                    }
                    .instrument(tracing::info_span!(
                        "job_run",
                        otel.name = format!("Cronjob run: {}", job_name_trace),
                        job.name = job_name_trace,
                    )),
                );
            }
        });

        self.register_job(job_name, job).await?;

        Ok(())
    }

    /// Add an async job to the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    /// * `schedule` - The schedule of the job
    /// * `run` - The async function to run
    #[tracing::instrument(skip(self, run), err)]
    pub async fn add_async_job<F, Fut>(
        &self,
        job_name: &str,
        schedule: Schedule,
        run: F,
    ) -> Result<(), Error<D>>
    where
        F: 'static + Sync + Send + Fn() -> Fut,
        Fut: Future<Output = ()> + Send,
    {
        let run = Arc::new(run);

        let job = job_scheduler_ng::Job::new(schedule, {
            let job_name = job_name.to_string();
            let np = Arc::clone(&self.node_pool);
            let run = Arc::clone(&run);

            let job_name_trace = job_name.clone();

            move || {
                let job_name = job_name.clone();
                let np = Arc::clone(&np);
                let run = Arc::clone(&run);

                // spawn the async job
                tokio::spawn(
                    async move {
                        // check if the job is available
                        if np
                            .check_job_available(&job_name)
                            .await
                            .is_ok_and(|is_this_node| is_this_node)
                        {
                            run().await;
                        }
                    }
                    .instrument(tracing::info_span!(
                        "job_run",
                        otel.name = format!("Cronjob run: {}", job_name_trace),
                        job.name = job_name_trace,
                    )),
                );
            }
        });

        self.register_job(job_name, job).await?;

        Ok(())
    }

    /// Remove a job from the cron
    ///
    /// # Arguments
    ///
    /// * `job_name` - The unique name of the job
    #[tracing::instrument(skip(self), err)]
    pub async fn remove_job(
        &self,
        job_name: &str,
    ) -> Result<(), Error<D>> {
        if let Some(id) = self.jobs.lock().await.remove(job_name) {
            self.scheduler.lock().await.remove(id);

            tracing::info!(
                name: "job_removed",
                message = format!("Job {} removed from scheduler", job_name),
                scheduler.uuid = id.to_string(),
                job.name = job_name,
            );
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
