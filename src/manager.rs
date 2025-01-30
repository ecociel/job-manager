use std::cmp::PartialEq;
use crate::error::JobError;
use crate::executor::JobExecutor;
use crate::jobs::{JobCfg, JobMetadata, JobStatus};
use crate::JobName;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;
use tokio::sync::Mutex;
use log::{info, warn};
use crate::repo::Repo;
use crate::schedule::JobSchedule;

type JobFn = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct Manager<R>
where
    R: Repo + Send + Sync + 'static,
{
    job_instance: String, // TODO: NEVER USED
    repo: Arc<R>,
    pub job_executor:  Arc<JobExecutor<R>>,
    jobs: Arc<Mutex<Vec<(JobCfg, JobFn)>>>,
}


impl<R: Repo + Sync + Send + 'static> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        let repo_arc = Arc::new(repo);
        let job_executor = Arc::new(JobExecutor {
            id: "executor_1".to_string(), //TODO - Need to fix this!!
            jobs: Arc::new(Mutex::new(vec![])),
            repository: repo_arc.clone(),
        });
        Manager {
            job_instance,
            repo: repo_arc.clone(),
            job_executor,
            jobs: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn register<F, Fut>(&mut self, config: JobCfg, job_func: F)
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let job_func_arc: JobFn = Arc::new(move |input| Box::pin(job_func(input)));
        let mut jobs = self.jobs.lock();
        jobs.await.push((config.clone(), job_func_arc));
        eprintln!("Job '{}' registered successfully.", config.name);
    }

    pub async fn run<F, Fut>(&mut self, job_name: &JobName, job_cfg: JobCfg, job_func: F, ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output=Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let job_entry = {
            let jobs = self.jobs.lock().await;
            jobs.iter()
                .find(|(cfg, _)| &cfg.name == job_name)
                .cloned()
        };

        if let Some((job_cfg, job_func)) = job_entry {
            job_cfg.validate()?;

            let job_metadata = JobMetadata {
                name: job_cfg.name.clone(),
                check_interval: job_cfg.check_interval,
                lock_ttl: job_cfg.lock_ttl,
                schedule: job_cfg.schedule.clone(),
                state: Arc::new(Mutex::new(vec![0])), //TODO: This is not used properly
                last_run: Utc::now(),
                retry_attempts: job_cfg.retry_attempts,
                max_retries: job_cfg.max_retries,
                backoff_duration: job_cfg.backoff_duration,
                status: JobStatus::Initializing,
            };
            let job_executor = self.job_executor.clone();
            job_executor
                .create_job(job_metadata.clone())
                .await
                .map_err(|e| {
                    JobError::SchedulerError(format!("Failed to add job to executor: {:?}", e))
                })?;
            Ok(())
                } else {
            Err(JobError::JobExecutionFailed(format!(
                "Job '{}' not found. Did you forget to register it?",
                job_name
            )))
        }
    }


    pub async fn start<F>(&self,job_func: F) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>> + Send +Copy + Sync + Clone + 'static,
    {
        info!("Starting all scheduled jobs...");
        match self.job_executor.start(job_func).await {
            Ok(_) => {
                info!("All jobs started successfully");
                Ok(())
            }
            Err(e) => {
                Err(JobError::JobExecutionFailed(format!("Failed to start jobs: {}", e)))
            }
        }
    }
}