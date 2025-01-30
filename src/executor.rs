use std::future::Future;
use std::pin::Pin;
use crate::JobMetadata;
use chrono::Utc;
use std::sync::Arc;
use log::warn;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::error::JobError;
use crate::jobs::JobStatus;
use crate::repo::Repo;


pub struct JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub(crate) id: String,  // TODO NOT USED
    //scheduler: Arc<Mutex<Scheduler>>, // TODO NOT USED
    pub(crate) jobs: Arc<Mutex<Vec<JobMetadata>>>,
    pub(crate) repository: Arc<R>,
}

impl<R>JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub fn new(repository: Arc<R>) -> Self {
        JobExecutor {
            id:"".to_string(),
            //scheduler,
            jobs: Arc::new(Mutex::new(Vec::new())),
            repository,
        }
    }
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

    pub async fn execute_job_with_retries<F, Fut>(
        &self,
        job_metadata: JobMetadata,
        job_func: F,
    ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let repo = self.repository.clone();
        let mut state = job_metadata.state.lock().await;

        let mut result = job_func(state.clone()).await;
        let mut attempts = 0;

        while result.is_err() && attempts < job_metadata.max_retries {
            attempts += 1;
            let status = JobStatus::Retrying;
            repo.save_and_commit_state(&job_metadata.name, status.clone())
                .await
                .map_err(|e| JobError::JobExecutionFailed(e.to_string()))?;

            if let Some(err) = result.as_ref().err() {
                warn!(
                    "Job '{:?}' attempt {} failed: {}. Retrying after {:?}...",
                    job_metadata.name, attempts, err, job_metadata.backoff_duration
                );
            }
            tokio::time::sleep(job_metadata.backoff_duration).await;
            result = job_func(state.clone()).await;
        }

        result.map(|_| ()).map_err(|e| JobError::JobExecutionFailed(format!("{}", e)))
    }


    pub async fn start<F>(&self, job_func: F) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> + Send + Copy + Sync + Clone + 'static,
    {
        let repository = self.repository.clone();

        loop {
            let now = Utc::now();
            let jobs = self.jobs.lock().await.clone();

            for job in jobs.iter() {
                if job.due(now) {
                    if let Err(err) = self.repository.release_all_locks().await {
                        eprintln!("Warning: Failed to release previous locks for job {:?}: {}", &job.name.0, err);
                    }
                    let lock_acquired = self.repository
                        .acquire_lock(&job.name.0)
                        .await
                        .map_err(|e| JobError::JobExecutionFailed(format!("Failed to acquire lock: {}", e)))?;

                    if lock_acquired {
                        let repository_clone = self.repository.clone();
                        let mut job_clone = job.clone();
                        let state_clone = job.state.clone();
                        let job_func_clone = job_func.clone();

                        tokio::spawn(async move {
                            let result = async {
                                let mut state = state_clone.lock().await.clone();
                                let schedule = job_clone.schedule.clone();
                                let mut last_run = job_clone.last_run;

                                repository_clone
                                    .save_and_commit_state(&job_clone.name, JobStatus::Running)
                                    .await
                                    .map_err(|e| JobError::JobExecutionFailed(format!("Failed to update job to Running: {}", e)))?;
                                eprintln!("******************************{}", schedule);
                                job_clone.run(&mut state, &mut last_run,&schedule , job_func_clone.clone()).await?;

                                repository_clone
                                    .save_and_commit_state(&job_clone.name, JobStatus::Completed)
                                    .await
                                    .map_err(|e| JobError::JobExecutionFailed(format!("Failed to update job to Completed: {}", e)))?;

                                Ok::<(), JobError>(())
                            };
                            if let Err(e) = result.await {
                                eprintln!("Job execution error: {}", e);
                            }
                            if let Err(err) = repository_clone.release_lock(&job_clone.name.0).await {
                                eprintln!("Failed to release lock for job {:?}: {}", &job_clone.name.0, err);
                            }
                        });
                    }
                }
            }

            sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    // pub async fn wait_for_shutdown(repository: Arc<R>) -> Result<(), JobError> {
    //     signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
    //
    //     eprintln!("Shutting down... Releasing all locks.");
    //     repository.release_all_locks().await.map_err(|e| {
    //         JobError::JobExecutionFailed(format!("Failed to release locks during shutdown: {}", e))
    //     })?;
    //
    //     eprintln!("All locks released. Exiting.");
    //     Ok(())
    // }
}