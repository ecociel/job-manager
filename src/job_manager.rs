use crate::scheduler::Scheduler;
use anyhow::Error;
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;

pub struct Manager<R> {
    job_instance: String,
    repo: Arc<R>,
    scheduler: Arc<Scheduler>,
}

#[derive(Debug)]
pub struct JobCfg {
    pub check_sec: u64,
    pub lock_ttl_sec: u64,
    pub schedule: String,
    pub enabled: bool,
}

pub struct JobInfo {
    // TODO : I need to think more on this more details of the job needed
    pub state: Vec<u8>,
}

#[async_trait]
pub trait JobsRepo {
    async fn get_job_info(&self, name: &str) -> Option<JobInfo>;
    async fn save_state(&self, name: &str, state: Vec<u8>) -> anyhow::Result<()>;
    async fn commit(&self, name: &str, state: Vec<u8>) -> anyhow::Result<()>;
    async fn create_job(&self, name: &str, job_cfg: JobCfg) -> anyhow::Result<()>;
}

impl<R: JobsRepo> Manager<R> {
    pub fn new(job_instance: String, repo: R) -> Self {
        Manager {
            job_instance,
            repo: Arc::new(repo),
            scheduler: Arc::new(Scheduler()),
        }
    }
    // This will help to refactor later
    // Jan's comment -  think overall you need to make use of two things you can maybe fix as preconditions
    // the whole execution loop could be done single threadedly
    // job never needs to be clone - no need for two owners at the same time
    // maybe such thinking helps to eliminate some of the concurrency and ownership issues

    pub async fn register<F, Fut>(
        &self,
        name: String,
        schedule: String,
        job_func: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
    {
        //TODO: Job is not implemented yet
        //let job = job::new(schedule.as_str(), move |_uuid, _l| {
        let repo = self.repo.clone();
        let name = name.clone();
        async move {
            if let Some(job_info) = repo.get_job_info(&name).await {
                if let Ok(new_state) = job_func(job_info.state).await {
                    repo.save_state(&name, new_state.clone()).await?;
                    repo.commit(&name, new_state).await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        };
        //});

        //self.scheduler.add(job).await?;

        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        //TODO: Implement Scheduler
        println!("Starting all scheduled jobs...");
        //self.scheduler.start().await?;
        Ok(())
    }
}
