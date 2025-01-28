use std::future::Future;
use std::pin::Pin;
use crate::scheduler::Scheduler;
use crate::JobMetadata;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::error::JobError;
use crate::repo::Repo;


pub struct JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    id: String,  // TODO NOT USED
    scheduler: Arc<Mutex<Scheduler>>, // TODO NOT USED
    jobs: Arc<Mutex<Vec<JobMetadata>>>,
    repository: Arc<R>,
}

impl<R>JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub fn new(scheduler: Arc<Mutex<Scheduler>>, repository: Arc<R>) -> Self {
        JobExecutor {
            id:"".to_string(),
            scheduler,
            jobs: Arc::new(Mutex::new(Vec::new())),
            repository,
        }
    }
    pub async fn add_job(&self, job_metadata: JobMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut jobs = self.jobs.lock().await;
        jobs.push(job_metadata.clone());
        self.repository.create_job(
            &job_metadata.name,
            job_metadata.backoff_duration,
            job_metadata.check_interval,
            job_metadata.last_run,
            job_metadata.lock_ttl,
            job_metadata.max_retries,
            job_metadata.retry_attempts,
            job_metadata.schedule.clone(),
            job_metadata.state.clone(),
        )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    pub async fn start<F>(&self, job_func: F) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>> + Send +Copy + Sync + Clone + 'static,
    {
        loop {
            let now = Utc::now();
            let jobs = self.jobs.lock().await.clone();

            for job in jobs.iter() {
                if job.due(now) {
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
                            let state_lock = state_clone.lock().await;
                            let mut state = state_lock.clone();

                            let schedule = job_clone.schedule.clone();
                            let mut last_run = job_clone.last_run;
                            let result = job_clone
                                .run(&mut state, &mut last_run, &schedule, job_func_clone.clone())
                                .await;
                            if result.is_err() {
                                eprintln!("Error executing job {:?}: {}", job_clone.name, result.unwrap_err());
                            } else {
                                eprintln!("Job {:?} completed successfully", job_clone.name);
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
}