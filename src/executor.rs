use std::future::Future;
use std::pin::Pin;
use crate::JobMetadata;
use chrono::Utc;
use std::sync::Arc;
use log::warn;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use crate::error::JobError;
use crate::jobs::JobStatus;
use crate::manager::Manager;
use crate::repo::Repo;


pub struct JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub(crate) id: String,  // TODO NOT USED - Do we need this. If yes why ?
    //TODO And do we need a separate Table to maintain the executor ?
    pub(crate) jobs: Arc<Mutex<Vec<JobMetadata>>>,
    pub(crate) repository: Arc<R>,
}

impl<R>JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub async fn create_job(&self, job_metadata: JobMetadata) -> Result<(), JobError> {
        let mut jobs = self.jobs.lock().await;
        jobs.push(job_metadata.clone());
        self.repository
            .create_job(
                &job_metadata.name,
                job_metadata.backoff_duration,
                job_metadata.check_interval,
                job_metadata.last_run,
                job_metadata.lock_ttl,
                job_metadata.max_retries,
                job_metadata.retry_attempts,
                job_metadata.schedule.clone(),
                job_metadata.state.clone(),
                job_metadata.status.clone(),
            )
            .await
            .map_err(|e| JobError::DatabaseError(format!("Failed to create job: {}", e)))?;

        Ok(())
    }

    pub async fn start<F>(&self, job_func: F) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>>
        + Send
        + Sync
        + Clone
        + 'static,
    {
        let jobs = self.jobs.lock().await.clone();

        if jobs.is_empty() {
            eprintln!("No jobs found to execute.");
            return Ok(());
        }
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for job in jobs.iter() {
            let repository_clone = self.repository.clone();
            let mut job_clone = job.clone();
            let state_clone = job.state.clone();
            let job_func_clone = job_func.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let now = Utc::now();
                    if job_clone.due(now) {
                        let lock_acquired = repository_clone
                            .acquire_lock(&job_clone.name.0)
                            .await
                            .unwrap_or(false);

                        eprintln!("Lock acquired for job {:?}: {:?}", job_clone.name, lock_acquired);

                        if lock_acquired {
                            let result = async {
                                let mut state = state_clone.lock().await.clone();
                                let mut last_run = job_clone.last_run;
                                let mut attempt = 0;
                                let max_retries = job_clone.max_retries;

                                loop {
                                    let job_execution = timeout(
                                        job_clone.lock_ttl,
                                        job_func_clone(state.clone())
                                    ).await;

                                    match job_execution {
                                        Ok(Ok(new_state)) => {
                                            state = new_state.clone();
                                            last_run = Utc::now();

                                            repository_clone
                                                .save_and_commit_state(&job_clone.name, JobStatus::Completed, new_state.clone())
                                                .await
                                                .map_err(|e| JobError::JobExecutionFailed(format!(
                                                    "Failed to update job to Completed: {}", e
                                                )))?;
                                            return Ok::<(), JobError>(());
                                        }
                                        Ok(Err(e)) if attempt < max_retries => {
                                            attempt += 1;
                                            eprintln!(
                                                "Job {:?} failed, retrying {}/{}: {}",
                                                job_clone.name, attempt, max_retries, e
                                            );
                                        }
                                        Ok(Err(e)) => {
                                            eprintln!("Job {:?} failed after {} attempts: {}", job_clone.name, max_retries, e);
                                            repository_clone
                                                .save_and_commit_state(&job_clone.name, JobStatus::Failed(e.to_string()), state)
                                                .await
                                                .map_err(|e| JobError::JobExecutionFailed(format!(
                                                    "Failed to update job to Failed: {}", e
                                                )))?;
                                            return Err(JobError::JobExecutionFailed(
                                                format!("Job {:?} failed permanently: {}", job_clone.name, e),
                                            ));
                                        }
                                        Err(e) => {
                                            eprintln!("Job {:?} timed out", &job_clone.name.0);
                                            repository_clone
                                                .save_and_commit_state(&job_clone.name, JobStatus::Failed(e.to_string()), state)
                                                .await
                                                .map_err(|e| JobError::JobExecutionFailed(format!(
                                                    "Failed to update job to Failed: {}", e
                                                )))?;
                                            return Err(JobError::JobExecutionFailed(
                                                format!("Job {:?} timed out", job_clone.name),
                                            ));
                                        }
                                    }
                                }
                            };
                            if let Err(e) = result.await {
                                eprintln!("Job execution error: {}", e);
                            }
                            if let Err(err) = repository_clone.release_lock(&job_clone.name.0).await {
                                eprintln!("Failed to release lock for job {:?}: {}", &job_clone.name.0, err);
                            }
                        }
                    }
                    sleep(job_clone.check_interval).await;
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            let _ = handle.await;
        }
        Ok(())
    }
}