use crate::jobs::JobMetadata;
use crate::scheduler::Scheduler;
use crate::{jobs, JobName};
use async_trait::async_trait;
use cron::Schedule;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
#[derive(Clone)]
pub struct Manager<R> {
    job_instance: String,
    repo: Arc<R>,
    scheduler: Arc<Mutex<Scheduler>>,
}

#[derive(Debug)]
pub struct JobCfg {
    pub name: JobName,
    pub check_interval: Duration,
    pub lock_ttl: Duration,
    pub schedule: Schedule,
}

#[async_trait]
pub trait JobsRepo {
    async fn get_job_info(&self, name: &str) -> Option<JobMetadata>;
    async fn save_state(&self, name: &str, state: Vec<u8>) -> anyhow::Result<()>;
    async fn commit(&self, name: &str, state: Vec<u8>) -> anyhow::Result<()>;
    async fn create_job(&self, name: &str, job_cfg: JobCfg) -> anyhow::Result<()>;
}

impl<R: JobsRepo + Sync + Send + 'static> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        Manager {
            job_instance,
            repo: Arc::new(repo),
            scheduler: Arc::new(Mutex::new(Scheduler::new())),
        }
    }
    // This will help to refactor later
    // Jan's comment -  think overall you need to make use of two things you can maybe fix as preconditions
    // the whole execution loop could be done single threadedly
    // job never needs to be clone - no need for two owners at the same time
    // maybe such thinking helps to eliminate some of the concurrency and ownership issues

    pub async fn register<F, Fut>(
        &mut self,
        name: String,
        job_cfg: JobCfg,
        job_func: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
    {
        //TODO: Job is not implemented yet
        let repo = self.repo.clone();
        let job_func = job_func.clone();
        let name = name.clone();
        let scheduler = self.scheduler.clone();
        let mut scheduler = scheduler.lock().await;
        let job = jobs::new(job_cfg, move |_uuid| {
            let repo = repo.clone();
            let name = name.clone();
            let value = job_func.clone();
            async move {
                if let Some(job_info) = repo.get_job_info(&name).await {
                    let state = job_info.state.lock().unwrap().clone();
                    if let Ok(new_state) = value(state).await {
                        repo.save_state(&name, new_state.clone()).await?;
                        repo.commit(&name, new_state.clone()).await?;
                        Ok(new_state)
                    } else {
                        Err(anyhow::anyhow!("Job function failed").into())
                    }
                } else {
                    Err(anyhow::anyhow!("Job info not found").into())
                }
            }
        });

        let job = || async { Ok(()) };
        scheduler
            .add(move || {
                let job = job.clone();
                Box::pin(async move { job().await })
            })
            .await?;
        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        //TODO: Implement Scheduler
        println!("Starting all scheduled jobs...");
        //self.scheduler.start().await?;
        Ok(())
    }
}
