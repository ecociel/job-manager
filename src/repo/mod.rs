use async_trait::async_trait;
use crate::{JobCfg, JobMetadata, JobName};
use crate::cassandra::RepoError;

pub mod cassandra;



#[async_trait]
pub trait Repo {
    // Asynchronously get job information (e.g., metadata)
    async fn get_job_info(&self, name: &JobName) -> Option<JobMetadata>;

    // Asynchronously save the state of a job
    async fn save_state(&self, name: &JobName, state: Vec<u8>) -> Result<(), RepoError>;

    // Asynchronously commit the state of a job (commit a transaction or some similar operation)
    async fn commit(&self, name: &JobName, state: Vec<u8>) -> Result<(), RepoError>;

    // Asynchronously create a new job
    async fn create_job(&self, name: &JobName, job_cfg: JobCfg) -> Result<(), RepoError>;
}
