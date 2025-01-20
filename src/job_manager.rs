use crate::scheduler::Scheduler;
use async_trait::async_trait;
use std::sync::Arc;

pub struct Manager<R> {
    job_instance: String,
    repo: Arc<R>,
    scheduler: Arc<Scheduler>,
}

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
    pub fn register() {}

    pub async fn start() {}
}
