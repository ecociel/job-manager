use crate::error::JobError;
use crate::executor::JobExecutor;
use crate::jobs::{JobCfg, JobMetadata};
use crate::scheduler::Scheduler;
use crate::JobName;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use crate::repo::Repo;

#[derive(Clone)]
pub struct Manager<R>
where
    R: Repo + Send + Sync + 'static,
{
    job_instance: String, // TODO: NEVER USED
    repo: Arc<R>,
    pub scheduler: Arc<Mutex<Scheduler>>,
    pub job_executor:  Arc<JobExecutor<R>>,
}

impl<R: Repo + Sync + Send + 'static> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        let repo_arc = Arc::new(repo);
        Manager {
            job_instance,
            repo: repo_arc.clone(),
            scheduler: Arc::new(Mutex::new(Scheduler::new())),
            job_executor: Arc::new(JobExecutor::new(Arc::new(Mutex::new(Scheduler::new())), repo_arc.clone())),
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
        Fut: Future<Output=anyhow::Result<Vec<u8>>> + Send + 'static,
    {
        job_cfg.validate()?;
        let repo = self.repo.clone();
        let scheduler = self.scheduler.clone();
        let scheduler = scheduler.lock().await;
        let job_executor = self.job_executor.clone();

        let initial_state = if let Ok(job_metadata) = repo.get_job_info(&job_name).await {
            eprintln!("Fetched job metadata: {:?}", job_metadata.state);
            job_metadata.state.lock().await.clone()
        } else {
            eprintln!("Job not found, using default state in-memory");
            b"initializing".to_vec()
        };
        let job_metadata = JobMetadata {
            name: job_name.clone(),
            check_interval: job_cfg.check_interval,
            lock_ttl: job_cfg.lock_ttl,
            schedule: job_cfg.schedule.clone(),
            state: Arc::new(Mutex::new(initial_state)),
            last_run: chrono::Utc::now(),
            retry_attempts: 0,
            max_retries: 3,
            backoff_duration: Duration::from_secs(2),
        };

        job_executor
            .add_job(job_metadata.clone())
            .await
            .expect("Failed to add job to executor");

        let job = Arc::new(move || {
            let job_metadata = job_metadata.clone();
            let repo = repo.clone();
            let job_func = job_func.clone();

            async move {
                let state = job_metadata.state.lock().await.clone();
                let mut result = job_func(state.clone()).await;
                let mut attempts = 0;

                while result.is_err() && attempts < job_metadata.max_retries {
                    attempts += 1;
                    let err = result.as_ref().err().unwrap();
                    eprintln!(
                        "Job '{:?}' attempt {} failed: {}. Retrying after {:?}...",
                        job_metadata.name, attempts, err, job_metadata.backoff_duration
                    );
                    tokio::time::sleep(job_metadata.backoff_duration).await;
                    result = job_func(state.clone()).await;
                }

                match result {
                    Ok(mut new_state) => {
                        new_state = b"completed".to_vec();
                        repo.save_and_commit_state(&job_metadata.name, new_state.clone())
                            .await
                            .map_err(|e| JobError::JobExecutionFailed(format!("Failed to save state: {}", e)))?;
                        eprintln!("Job '{:?}' Registered successfully", job_metadata.name);
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!(
                            "Job '{:?}' failed after {} retries: {}",
                            job_metadata.name, attempts, e
                        );
                        Err(JobError::JobExecutionFailed(format!(
                            "Job '{:?}' failed: {}",
                            job_metadata.name, e
                        )))
                    }
                }
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

pub async fn run<F>(&self,job_func: F) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>> + Send +Copy + Sync + Clone + 'static,
    {
        println!("Starting all scheduled jobs...");
        let result =self.job_executor.start(job_func).await;
        if let Err(e) = result {
            eprintln!("Failed to start job executor: {:?}", e);
            return Err(e);
        }
        Ok(())
    }
}