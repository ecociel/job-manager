use std::cmp::PartialEq;
use crate::error::JobError;
use crate::executor::JobExecutor;
use crate::jobs::{JobCfg, JobMetadata, JobStatus};
use crate::scheduler::Scheduler;
use crate::JobName;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;
use tokio::sync::Mutex;
use log::{info, warn};
use crate::repo::Repo;

type JobFn = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct Manager<R>
where
    R: Repo + Send + Sync + 'static,
{
    job_instance: String, // TODO: NEVER USED
    repo: Arc<R>,
    pub scheduler: Arc<Mutex<Scheduler>>,
    pub job_executor:  Arc<JobExecutor<R>>,
    jobs: Arc<Mutex<Vec<(JobCfg, JobFn)>>>,
}


impl<R: Repo + Sync + Send + 'static> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        let repo_arc = Arc::new(repo);
        Manager {
            job_instance,
            repo: repo_arc.clone(),
            scheduler: Arc::new(Mutex::new(Scheduler::new())),
            job_executor: Arc::new(JobExecutor::new(Arc::new(Mutex::new(Scheduler::new())), repo_arc.clone())),
            jobs: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn register<F, Fut>(&mut self, config: JobCfg, job_func: F)
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let job_func_arc: JobFn = Arc::new(move |input| Box::pin(job_func(input)));
        let mut jobs = self.jobs.lock();
        jobs.await.push((config.clone(), job_func_arc));

        eprintln!("Job '{}' registered successfully.", config.name);
    }

    pub async fn run_registered(&mut self, job_name: &JobName) -> Result<(), JobError> {

        let job_entry = {
            let jobs = self.jobs.lock().await;
            jobs.iter()
                .find(|(cfg, _)| &cfg.name == job_name)
                .cloned()
        };

        if let Some((job_cfg, job_func)) = job_entry {
            let job_func_cloned = job_func.clone();
            self.run(job_cfg.clone(), move |input| {
                let fut = job_func_cloned(input);
                Box::pin(async move {
                    fut.await.map_err(|e| anyhow::Error::msg(e.to_string()))
                })
            }).await
        } else {
            Err(JobError::JobExecutionFailed(format!(
                "Job '{}' not found. Did you forget to register it?",
                job_name
            )))
        }
    }



    // This will help to refactor later
    // Jan's comment -  think overall you need to make use of two things you can maybe fix as preconditions
    // the whole execution loop could be done single threadedly
    // job never needs to be clone - no need for two owners at the same time
    // maybe such thinking helps to eliminate some of the concurrency and ownership issues

    pub async fn run<F, Fut>(
        &mut self,
        job_cfg: JobCfg,
        job_func: F,
    ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
    {

        job_cfg.validate()?;
        let repo = self.repo.clone();
        let scheduler = self.scheduler.clone();
        let mut scheduler = scheduler.lock().await;
        let job_executor = self.job_executor.clone();

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
        };

        job_executor
            .add_job(job_metadata.clone())
            .await
            .map_err(|e| {
                JobError::SchedulerError(format!("Failed to add job to executor: {:?}", e))
            })?;

        let job = Arc::new(move || {
            let job_metadata = job_metadata.clone();
            let repo = repo.clone();
            let job_func = job_func.clone();

            async move {
                let mut state = job_metadata.state.lock().await;
                //  let status = JobStatus::Running;
                // repo.save_and_commit_state(&job_metadata.name, status.clone()).await?;

                let mut result = job_func(state.clone()).await;
                let mut attempts = 0;

                while result.is_err() && attempts < job_metadata.max_retries {
                    attempts += 1;
                    let status = JobStatus::Retrying;
                    repo.save_and_commit_state(&job_metadata.name, status.clone()).await?;

                    let err = result.as_ref().err().unwrap();
                    warn!(
                        "Job '{:?}' attempt {} failed: {}. Retrying after {:?}...",
                        job_metadata.name, attempts, err, job_metadata.backoff_duration
                    );
                    tokio::time::sleep(job_metadata.backoff_duration).await;
                    result = job_func(state.clone()).await;
                }

                // if result.is_ok() {
                //     let status = JobStatus::Completed;
                // } else {
                //     let status = JobStatus::Failed;
                // }
                //
                // repo.save_and_commit_state(&job_metadata.name, status.clone()).await?;
                result.map(|_| ()).map_err(|e| JobError::JobExecutionFailed(format!("{}", e)))
            }
        });

        scheduler
            .add(move || {
                let job = job.clone();
                Box::pin(async move {
                    job().await.map_err(|e| anyhow::Error::msg(format!("{}", e)))
                })
            })
            .await
            .map_err(|e| JobError::SchedulerError(format!("{}", e)))?;

        Ok(())
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
                Err(e)
            }
        }
    }
}