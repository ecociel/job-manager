use crate::error::JobError;
use crate::executor::JobExecutor;
use crate::jobs::{JobCfg, JobMetadata, JobStatus};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use chrono::Utc;
use tokio::sync::Mutex;
use log::info;
use crate::repo::Repo;

type JobFn = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct Manager<R>
where
    R: Repo + Send + Sync + 'static,
{
    repo: Arc<R>,
    pub job_executor:  Arc<JobExecutor<R>>,
    pub(crate) jobs: Arc<Mutex<Vec<(JobCfg,JobFn)>>>,
}


impl<R: Repo + Sync + Send + 'static> Manager<R> {
    pub fn new(repo: R) -> Self {
        let repo_arc = Arc::new(repo);
        let jobs = Arc::new(Mutex::new(vec![]));
        let job_executor = Arc::new(JobExecutor {
            jobs: jobs.clone(),
            repository: repo_arc.clone(),
        });
        Manager {
            repo: repo_arc.clone(),
            job_executor,
            jobs: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn register<F,Fut>(&mut self, job_cfg: JobCfg, job_func: F)
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let job_func_arc: JobFn = Arc::new(move |input| Box::pin(job_func(input)));
        let mut jobs = self.jobs.lock().await;
        jobs.push((job_cfg.clone(), job_func_arc));

        job_cfg.validate().unwrap();

        let job_metadata = JobMetadata {
            name: job_cfg.name.clone(),
            check_interval: job_cfg.check_interval,
            lock_ttl: job_cfg.lock_ttl,
            schedule: job_cfg.schedule.clone(),
            state: Arc::new(Mutex::new(vec![0])),
            last_run: Utc::now(),
            retry_attempts: job_cfg.retry_attempts,
            max_retries: job_cfg.max_retries,
            backoff_duration: job_cfg.backoff_duration,
            status: JobStatus::Initializing,
            timeout: job_cfg.timeout,
        };

        let job_executor = self.job_executor.clone();
        job_executor.create_job(job_metadata.clone()).await.map_err(|e| {
            JobError::SchedulerError(format!("Failed to add job to executor: {:?}", e))
        }).unwrap();
        eprintln!("Job Registered");
    }

    pub async fn start<F>(&self,job_func: F) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>> + Send + Sync + Clone + 'static,
    {
        eprintln!("Starting all scheduled jobs...");
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