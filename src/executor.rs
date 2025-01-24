use crate::scheduler::Scheduler;
use crate::JobMetadata;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::cassandra::TheRepository;
use crate::repo::Repo;


pub struct JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    scheduler: Arc<Mutex<Scheduler>>,
    jobs: Arc<Mutex<Vec<JobMetadata>>>,
    repository: Arc<R>,
}

impl<R>JobExecutor<R>
where
    R: Repo + Send + Sync + 'static,
{
    pub fn new(scheduler: Arc<Mutex<Scheduler>>,repository: Arc<R>) -> Self {
        JobExecutor {
            scheduler,
            jobs: Arc::new(Mutex::new(Vec::new())),
            repository,
        }
    }
    pub async fn add_job(&self, job_metadata: JobMetadata) ->  Result<(), Box<dyn std::error::Error>> {
        let mut jobs = self.jobs.lock().await;
        jobs.push(job_metadata.clone());
        self.repository
            .create_job(
                &job_metadata.name,
                job_metadata.check_interval,
                job_metadata.lock_ttl,
                job_metadata.schedule.clone(),
                job_metadata.state.clone(),
                job_metadata.last_run,
                job_metadata.retry_attempts,
                job_metadata.max_retries,
                job_metadata.backoff_duration,
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }
    

    pub async fn start(&self) {
        loop {
            let now = Utc::now();

            let jobs = self.jobs.lock().await.clone();

            for job in jobs.iter() {
                if job.due(now) {
                    let job_func = move |state: Vec<u8>| async { Ok(state) };
                    let mut job_clone = job.clone();
                    let state_clone = job.state.clone();
                    tokio::spawn(async move {
                        let state_lock = state_clone.lock().await;
                        let mut state = state_lock.clone();

                        let schedule = job_clone.schedule.clone();
                        let mut last_run = job_clone.last_run;
                        let result = job_clone
                            .run(&mut state, &mut last_run, &schedule, job_func)
                            .await;

                        if let Err(err) = result {
                            eprintln!("Error executing job {:?}: {}", job_clone.name, err);
                        } else {
                            eprintln!("Job {:?} completed successfully", job_clone.name);
                        }
                    });
                }
            }
            sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
