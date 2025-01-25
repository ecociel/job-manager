use crate::error::JobError;
use crate::executor::JobExecutor;
use crate::{jobs};
use crate::jobs::{JobCfg, JobMetadata};
use crate::scheduler::Scheduler;
use crate::JobName;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::repo::Repo;

#[derive(Clone)]
pub struct Manager<R>
where
    R: Repo + Send + Sync + 'static,
{
    job_instance: String,
    repo: Arc<R>,
    scheduler: Arc<Mutex<Scheduler>>,
    job_executor:  Arc<JobExecutor<R>>,
}

impl<R: Repo + Sync + Send + 'static> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        let repo_arc = Arc::new(repo);
        Manager {
            job_instance,
            repo: repo_arc.clone(),
            scheduler: Arc::new(Mutex::new(Scheduler::new())),
            job_executor: Arc::new(JobExecutor::new(Arc::new(Mutex::new(Scheduler::new())),repo_arc.clone())),
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
            max_retries: 3,
            backoff_duration: Duration::from_secs(2),
        };
        eprintln!("job_metadata {:?}", job_metadata);
        job_executor.add_job(job_metadata).await.expect("TODO: panic message");
        eprintln!("Job added");
        let job = jobs::new(job_cfg.clone(), move |_uuid| {
            let repo = repo.clone();
            let name = job_name.clone();
            let job_func = job_func.clone();
            async move {
                if let Ok(job_info) = repo.get_job_info(&name).await {
                    let state = job_info.state.lock().await.clone();

                    let mut result = job_func(state.clone()).await;
                    let mut attempts = 0;
                    while result.is_err() && attempts < job_info.max_retries {
                        attempts += 1;
                        sleep(job_info.backoff_duration).await;
                        result = job_func(state.clone()).await;
                    }

                    if let Err(e) = result {
                        Err(JobError::JobExecutionFailed(format!("{}", e)))
                    } else {
                        let new_state = result.unwrap();
                        repo.save_state(name.0.clone(), new_state.clone()).await?;
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