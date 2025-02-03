use std::future::Future;
use std::pin::Pin;
use crate::JobMetadata;
use chrono::{DateTime, MappedLocalTime, TimeZone, Utc};
use std::sync::Arc;
use log::{info, warn};
use tokio::{signal, spawn};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use crate::error::JobError;
use crate::jobs::JobStatus;
use crate::manager::Manager;
use crate::repo::Repo;
use tokio::time::Duration;


pub struct JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub(crate) id: String,  // TODO NOT USED - Do we need this. If yes why ?
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
            let repository_clone1 = repository_clone.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let job_name = job_clone.name.0.clone();
                    let now = Utc::now();

                    if job_clone.due(now) {
                        let lock_acquired = repository_clone1
                            .acquire_lock(&job_name)
                            .await
                            .unwrap_or(false);

                        eprintln!("Lock acquired for job {:?}: {:?}", job_clone.name, lock_acquired);

                        if lock_acquired {
                            let lock_ttl = job_clone.lock_ttl;
                            let lock_toucher_handle = tokio::spawn(Self::extend_lock(repository_clone.clone(), job_name.clone(), lock_ttl));

                            let result = tokio::select! {
                            res = Self::execute_job(job_clone.clone(), state_clone.clone(), job_func_clone.clone(), repository_clone.clone()) => res,
                            _ = sleep(job_clone.timeout) => {
                                eprintln!("Job {:?} timed out, releasing lock", job_clone.name);
                                Err(JobError::JobExecutionFailed(format!("Job {:?} timed out", job_clone.name)))
                            }
                        };
                            if let Err(e) = result {
                                eprintln!("Job execution error: {}", e);
                            }
                            lock_toucher_handle.abort();
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
        let monitoring_handle = tokio::spawn(Self::monitor_jobs(self.repository.clone(), self.jobs.clone()));
        for handle in handles {
            let _ = handle.await;
        }
        let _ = monitoring_handle.await;
        Ok(())
    }

    async fn execute_job<F>(
        job_clone: JobMetadata,
        state_clone: Arc<Mutex<Vec<u8>>>,
        job_func_clone: F,
        repository_clone: Arc<dyn Repo>,
    ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>>
        + Send
        + Sync
        + Clone
        + 'static,
    {
        let mut state = state_clone.lock().await.clone();
        let mut last_run = job_clone.last_run;
        let mut attempt = 0;
        let max_retries = job_clone.max_retries;

        loop {
            let job_execution = timeout(job_clone.timeout, job_func_clone(state.clone())).await;
            match job_execution {
                Ok(Ok(new_state)) => {
                    state = new_state.clone();
                    last_run = Utc::now();
                    repository_clone.save_and_commit_state(&job_clone.name, JobStatus::Completed, new_state.clone(), last_run).await?;
                    return Ok(());
                }
                Ok(Err(e)) if attempt < max_retries => {
                    attempt += 1;
                    eprintln!("Job {:?} failed, retrying {}/{}: {}", job_clone.name, attempt, max_retries, e);
                }
                Ok(Err(e)) => {
                    eprintln!("Job {:?} failed after {} attempts: {}", job_clone.name, max_retries, e);
                    repository_clone
                        .save_and_commit_state(&job_clone.name, JobStatus::Failed(e.to_string()), state, last_run)
                        .await
                        .map_err(|e| JobError::JobExecutionFailed(format!("Failed to update job to Failed: {}", e)))?;
                    return Err(JobError::JobExecutionFailed(format!("Job {:?} failed permanently: {}", job_clone.name, e)));
                }
                Err(_) => {
                    eprintln!("Job {:?} timed out", job_clone.name);
                    repository_clone
                        .save_and_commit_state(&job_clone.name, JobStatus::Failed("Timeout".to_string()), state, last_run)
                        .await
                        .map_err(|e| JobError::JobExecutionFailed(format!("Failed to update job to Failed: {}", e)))?;
                    return Err(JobError::JobExecutionFailed(format!("Job {:?} timed out", job_clone.name)));
                }
            }
        }
    }

    async fn extend_lock(repository: Arc<dyn Repo>, job_name: String, mut ttl: Duration) {
        let min_ttl = Duration::from_secs(1);

        loop {
            if ttl > min_ttl {
                let half_ttl = ttl / 2;
                let new_ttl = if half_ttl >= min_ttl {
                    half_ttl
                } else {
                    min_ttl
                };
                sleep(new_ttl).await;
                if let Err(e) = repository.update_lock_ttl(&job_name, new_ttl).await {
                    eprintln!("Error updating TTL for job {}: {}", job_name, e);
                    break;
                }
               eprintln!("Lock TTL updated for job {:?} to {:?}", job_name, new_ttl);
                ttl = new_ttl;
            } else {
                eprintln!("Lock TTL too small, stopping updates.");
                break;
            }
        }
    }

    async fn monitor_jobs(repository: Arc<dyn Repo>, jobs: Arc<Mutex<Vec<JobMetadata>>>) {
        loop {
            let jobs = jobs.lock().await;

            for job_metadata in jobs.iter() {
                let job_name = job_metadata.name.0.clone();

                if let Ok(Some(last_run)) = repository.get_last_run_time(&job_name).await {
                    let now = Utc::now();
                    let last_run_dt = Utc.timestamp_millis(last_run);

                    let elapsed = now - last_run_dt;

                    if let Ok(elapsed_duration) = elapsed.to_std() {
                        if elapsed_duration > job_metadata.lock_ttl {
                            if let Err(err) = repository.release_lock(&job_name).await {
                                eprintln!("Failed to release lock for job {:?}: {}", job_name, err);
                            }
                        }
                    } else {
                        info!(
                            "Warning: Negative elapsed time for job {:?}, skipping comparison.",
                            job_name
                        );
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

//TODO : Not used as of now
//     async fn monitor_jobs(repository: Arc<dyn Repo>, jobs: Arc<Mutex<Vec<JobMetadata>>>) {
//         loop {
//             let jobs = jobs.lock().await.clone();
//
//             for job_metadata in jobs.iter() {
//                 let job_name = job_metadata.name.0.clone();
//
//                 if let Ok(Some(last_run)) = repository.get_last_run_time(&job_name).await {
//                     let now = Utc::now();
//
//                     let last_run_dt_opt = Utc.timestamp_millis_opt(last_run);
//                     match last_run_dt_opt {
//                         chrono::LocalResult::Single(last_run_dt) => {
//                             let elapsed = now - last_run_dt;
//                             eprintln!(
//                                 "Job: {:?}, now: {:?}, last_run: {:?}, elapsed: {:?}",
//                                 job_name, now, last_run, elapsed
//                             );
//
//                             if let Ok(elapsed_duration) = elapsed.to_std() {
//                                 if elapsed_duration > job_metadata.lock_ttl {
//                                     eprintln!("Job {:?} appears stuck. Releasing lock.", job_name);
//                                     if let Err(err) = repository.release_lock(&job_name).await {
//                                         eprintln!("Failed to release lock for job {:?}: {}", job_name, err);
//                                     }
//                                 }
//                             } else {
//                                 eprintln!(
//                                     "Warning: Negative elapsed time for job {:?}, skipping comparison.",
//                                     job_name
//                                 );
//                             }
//                         },
//                         chrono::LocalResult::None => {
//                             eprintln!("Failed to convert last_run timestamp to DateTime<Utc> for job {:?}", job_name);
//                         }
//                         _ => {}
//                     }
//                 }
//             }
//             tokio::time::sleep(Duration::from_secs(10)).await;
//         }
//     }
// }

// pub async fn start<F>(&self, job_func: F) -> Result<(), JobError>
    // where
    //     F: Fn(Vec<u8>) -> Pin<Box<dyn Future<Output=Result<Vec<u8>, JobError>> + Send>>
    //     + Send
    //     + Sync
    //     + Clone
    //     + 'static,
    // {
    //     let jobs = self.jobs.lock().await.clone();
    //
    //     if jobs.is_empty() {
    //         eprintln!("No jobs found to execute.");
    //         return Ok(());
    //     }
    //     let mut handles: Vec<JoinHandle<()>> = Vec::new();
    //
    //     for job in jobs.iter() {
    //         let repository_clone = self.repository.clone();
    //         let mut job_clone = job.clone();
    //         let state_clone = job.state.clone();
    //         let job_func_clone = job_func.clone();
    //         let repository_clone1 = repository_clone.clone();//TODO Fix this
    //
    //
    //         let handle = tokio::spawn(async move {
    //             loop {
    //                 let job_name = job_clone.name.0.clone();
    //                 let now = Utc::now();
    //                 if job_clone.due(now) {
    //                     let lock_acquired = repository_clone1
    //                         .acquire_lock(&job_name)
    //                         .await
    //                         .unwrap_or(false); //TODO Fix this
    //
    //                     eprintln!("Lock acquired for job {:?}: {:?}", job_clone.name, lock_acquired);
    //                     let repository_clone2 = repository_clone.clone(); //TODO Fix this
    //                     if lock_acquired {
    //                         let lock_ttl = job_clone.lock_ttl;
    //                         let lock_toucher_handle = spawn(async move {
    //                             let mut ttl = lock_ttl;
    //                             let min_ttl = Duration::from_secs(1);
    //                             loop {
    //                                 if ttl > min_ttl {
    //                                     let half_ttl_secs = ttl.as_secs() / 2;
    //                                     let half_ttl_duration = Duration::from_secs(half_ttl_secs);
    //
    //                                     sleep(half_ttl_duration).await;
    //
    //                                     let update_result = repository_clone2.update_lock_ttl(&job_name, ttl).await;
    //                                     if let Err(e) = update_result {
    //                                         eprintln!("Error updating TTL for job {}", e);
    //                                     } else {
    //                                         eprintln!("Lock TTL updated for job to {:?}", ttl);
    //                                     }
    //                                 ttl /= 2;
    //                                     if ttl <= min_ttl {
    //                                         eprintln!("Lock TTL has reached minimum threshold of {:?}, stopping further updates", min_ttl);
    //                                         break;
    //                                     } } else {
    //                                     eprintln!("Lock TTL is too small to continue updating. Stopping.");
    //                                     break;
    //                                 }
    //                             }
    //                         });
    //
    //                         let result = async {
    //                             let mut state = state_clone.lock().await.clone();
    //                             let mut last_run = job_clone.last_run;
    //                             let mut attempt = 0;
    //                             let max_retries = job_clone.max_retries;
    //
    //                             loop {
    //                                 let job_execution = timeout(
    //                                     job_clone.timeout,
    //                                     job_func_clone(state.clone())
    //                                 ).await;
    //
    //                                 match job_execution {
    //                                     Ok(Ok(new_state)) => {
    //                                         state = new_state.clone();
    //                                         last_run = Utc::now();
    //
    //                                         repository_clone
    //                                             .save_and_commit_state(&job_clone.name, JobStatus::Completed, new_state.clone(),last_run)
    //                                             .await
    //                                             .map_err(|e| JobError::JobExecutionFailed(format!(
    //                                                 "Failed to update job to Completed: {}", e
    //                                             )))?;
    //                                         return Ok::<(), JobError>(());
    //                                     }
    //                                     Ok(Err(e)) if attempt < max_retries => {
    //                                         attempt += 1;
    //                                         eprintln!(
    //                                             "Job {:?} failed, retrying {}/{}: {}",
    //                                             job_clone.name, attempt, max_retries, e
    //                                         );
    //                                     }
    //                                     Ok(Err(e)) => {
    //                                         eprintln!("Job {:?} failed after {} attempts: {}", job_clone.name, max_retries, e);
    //                                         repository_clone
    //                                             .save_and_commit_state(&job_clone.name, JobStatus::Failed(e.to_string()), state, last_run)
    //                                             .await
    //                                             .map_err(|e| JobError::JobExecutionFailed(format!(
    //                                                 "Failed to update job to Failed: {}", e
    //                                             )))?;
    //                                         return Err(JobError::JobExecutionFailed(
    //                                             format!("Job {:?} failed permanently: {}", job_clone.name, e),
    //                                         ));
    //                                     }
    //                                     Err(e) => {
    //                                         eprintln!("Job {:?} timed out", &job_clone.name.0);
    //                                         repository_clone
    //                                             .save_and_commit_state(&job_clone.name, JobStatus::Failed(e.to_string()), state,last_run)
    //                                             .await
    //                                             .map_err(|e| JobError::JobExecutionFailed(format!(
    //                                                 "Failed to update job to Failed: {}", e
    //                                             )))?;
    //                                         return Err(JobError::JobExecutionFailed(
    //                                             format!("Job {:?} timed out", job_clone.name),
    //                                         ));
    //                                     }
    //                                 }
    //                             }
    //                         };
    //                         if let Err(e) = result.await {
    //                             eprintln!("Job execution error: {}", e);
    //                         }
    //                         lock_toucher_handle.abort();
    //                         if let Err(err) = repository_clone.release_lock(&job_clone.name.0).await {
    //                             eprintln!("Failed to release lock for job {:?}: {}", &job_clone.name.0, err);
    //                         }
    //                     }
    //                 }
    //                 sleep(job_clone.check_interval).await;
    //             }
    //         });
    //         handles.push(handle);
    //     }
    //     for handle in handles {
    //         let _ = handle.await;
    //     }
    //     Ok(())
    // }
