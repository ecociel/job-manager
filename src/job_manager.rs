use crate::error::JobError;
use crate::executor::JobExecutor;
use crate::jobs;
use crate::jobs::{JobCfg, JobMetadata};
use crate::scheduler::Scheduler;
use crate::JobName;
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Clone)]
pub struct Manager<R> {
    job_instance: String,
    repo: Arc<R>,
    scheduler: Arc<Mutex<Scheduler>>,
    job_executor: Arc<JobExecutor>,
}

#[async_trait]
pub trait JobsRepo {
    async fn get_job_info(&self, name: &JobName) -> Option<JobMetadata>;
    async fn save_state(&self, name: &JobName, state: Vec<u8>) -> anyhow::Result<(), JobError>;
    async fn commit(&self, name: &JobName, state: Vec<u8>) -> anyhow::Result<(), JobError>;
    async fn create_job(&self, name: &JobName, job_cfg: JobCfg) -> anyhow::Result<(), JobError>;
}

impl<R: JobsRepo + Sync + Send + 'static> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        Manager {
            job_instance,
            repo: Arc::new(repo),
            scheduler: Arc::new(Mutex::new(Scheduler::new())),
            job_executor: Arc::new(JobExecutor::new(Arc::new(Mutex::new(Scheduler::new())))),
        }
    }
    // This will help to refactor later
    // Jan's comment -  think overall you need to make use of two things you can maybe fix as preconditions
    // the whole execution loop could be done single threadedly
    // job never needs to be clone - no need for two owners at the same time
    // maybe such thinking helps to eliminate some of the concurrency and ownership issues

    pub async fn register<F, Fut>(
        &mut self,
        job_name: JobName,
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
        let job_executor = self.job_executor.clone();

        let job_metadata = JobMetadata {
            name: job_name.clone(),
            check_interval: job_cfg.check_interval,
            lock_ttl: job_cfg.lock_ttl,
            schedule: job_cfg.schedule.clone(),
            state: Arc::new(Mutex::new(Vec::new())),
            last_run: chrono::Utc::now(),
            retry_attempts: 0,
            max_retries: 3,                           // Set default retry count
            backoff_duration: Duration::from_secs(2), // Default backoff duration
        };

        // Add the job to the executor for periodic execution
        job_executor.add_job(job_metadata).await;

        let job = jobs::new(job_cfg.clone(), move |_uuid| {
            let repo = repo.clone();
            let name = job_name.clone();
            let job_func = job_func.clone();
            async move {
                if let Some(job_info) = repo.get_job_info(&name).await {
                    let state = job_info.state.lock().await.clone();

                    let mut result = job_func(state.clone()).await;
                    let mut attempts = 0;
                    while result.is_err() && attempts < job_info.max_retries {
                        attempts += 1;
                        sleep(job_info.backoff_duration).await; // Apply backoff
                        result = job_func(state.clone()).await; // Retry job
                    }

                    if let Err(e) = result {
                        Err(JobError::JobExecutionFailed(format!("{}", e)))
                    } else {
                        let new_state = result.unwrap();
                        repo.save_state(&name, new_state.clone()).await?;
                        repo.commit(&name, new_state.clone()).await?;
                        Ok(new_state)
                    }
                } else {
                    Err(JobError::JobInfoNotFound(name.as_str().to_string()))
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
        self.job_executor.start().await;
        Ok(())
    }
}

//TODO I need to figure out this later
#[async_trait]
impl<T: JobsRepo + ?Sized + Sync + Send> JobsRepo for Arc<T> {
    async fn get_job_info(&self, name: &JobName) -> Option<JobMetadata> {
        (**self).get_job_info(name).await
    }

    async fn save_state(&self, name: &JobName, state: Vec<u8>) -> anyhow::Result<(), JobError> {
        (**self).save_state(name, state).await
    }

    async fn commit(&self, name: &JobName, state: Vec<u8>) -> anyhow::Result<(), JobError> {
        (**self).commit(name, state).await
    }

    async fn create_job(&self, name: &JobName, job_cfg: JobCfg) -> anyhow::Result<(), JobError> {
        (**self).create_job(name, job_cfg).await
    }
}
