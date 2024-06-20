use std::collections::HashMap;
use std::sync::Arc;

use job_scheduler::{Job, Schedule, Uuid};
use tokio::sync::Mutex;
use tokio::task;

use crate::node_pool;

pub struct Cron<'a> {
    node_pool: Arc<Mutex<node_pool::NodePool>>,
    jobs: Mutex<HashMap<String, Uuid>>,
    cron: Mutex<job_scheduler::JobScheduler<'a>>,
}

impl<'a> Cron<'a> {
    pub async fn new(node_pool: node_pool::NodePool) -> Result<Self, node_pool::Error> {
        let node_pool = Arc::new(Mutex::new(node_pool));
        node_pool.lock().await.start().await?;

        Ok(Self {
            node_pool,
            jobs: Mutex::new(HashMap::new()),
            cron: Mutex::new(job_scheduler::JobScheduler::new()),
        })
    }

    pub async fn add_job<T>(&self, name: String, schedule: Schedule, run: T) where T: FnMut() + Send + 'static {
        let node_pool = Arc::clone(&self.node_pool);

        let run = Arc::new(Mutex::new(run));
        let job = Job::new(
            schedule,
            {
                let name = name.clone();

                move || {
                    let name = name.clone();
                    let node_pool = node_pool.clone();
                    let run = run.clone();

                    task::spawn_local(async move {
                        if node_pool.blocking_lock_owned().check_job_available(name.clone()).await.is_ok_and(|b| b) {
                            let mut run = run.lock().await;
                            run();
                        }
                    });
                }
            },
        );

        let uuid = self.cron.lock().await.add(job);
        self.jobs.lock().await.insert(name.to_string(), uuid);
    }

    pub async fn remove_job(&self, name: &str) {
        if let Some(uuid) = self.jobs.lock().await.remove(name) {
            self.cron.lock().await.remove(uuid);
        }
    }

    pub async fn start(&self) -> Result<(), node_pool::Error> {
        self.node_pool.lock().await.start().await
    }
}


