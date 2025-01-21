use crate::error::JobError;
use crate::jobs;
use crate::jobs::{JobCfg, JobMetadata};
use crate::scheduler::Scheduler;
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;
#[derive(Clone)]
pub struct Manager<R> {
    job_instance: String,
    repo: Arc<R>,
    scheduler: Arc<Mutex<Scheduler>>,
}

#[async_trait]
pub trait JobsRepo {
    async fn get_job_info(&self, name: &str) -> Option<JobMetadata>;
    async fn save_state(&self, name: &str, state: Vec<u8>) -> anyhow::Result<(), JobError>;
    async fn commit(&self, name: &str, state: Vec<u8>) -> anyhow::Result<(), JobError>;
    async fn create_job(&self, name: &str, job_cfg: JobCfg) -> anyhow::Result<(), JobError>;
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
    ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
    {
        //TODO: Job is not implemented yet
        job_cfg.validate()?;
        let repo = self.repo.clone();
        let scheduler = self.scheduler.clone();
        let mut scheduler = scheduler.lock().await;
        let job = jobs::new(job_cfg, move |_uuid| {
            let repo = repo.clone();
            let name = name.clone();
            let value = job_func.clone();
            async move {
                if let Some(job_info) = repo.get_job_info(&name).await {
                    let state = job_info.state.lock().unwrap().clone();
                    match value(state).await {
                        Ok(new_state) => {
                            if let Err(_) = repo.save_state(&name, new_state.clone()).await {
                                return Err(JobError::SaveStateFailed(name));
                            }

                            if let Err(_) = repo.commit(&name, new_state.clone()).await {
                                return Err(JobError::CommitStateFailed(name));
                            }

                            Ok(new_state)
                        }
                        Err(e) => Err(JobError::JobExecutionFailed(format!("{}", e))),
                    }
                } else {
                    Err(JobError::JobInfoNotFound(name))
                }
            }
        });

        let job = || async { Ok(()) };
        scheduler
            .add(move || {
                let job = job.clone();
                Box::pin(async move { job().await })
            })
            .await
            .map_err(|e| JobError::SchedulerError(format!("{}", e)))?;
        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<(), JobError> {
        //TODO: Implement Scheduler
        println!("Starting all scheduled jobs...");
        //self.scheduler.start().await?;
        Ok(())
    }
}
