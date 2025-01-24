use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::sync::Mutex;
use crate::{JobCfg, JobMetadata, JobName};
use crate::cassandra::RepoError;

pub mod cassandra;



#[async_trait]
pub trait Repo {
 
    async fn get_job_info(&self, name: &JobName) -> Option<JobMetadata>;
    
    async fn save_state(&self, name: &JobName, state: Vec<u8>) -> Result<(), RepoError>;
    
    async fn commit(&self, name: &JobName, state: Vec<u8>) -> Result<(), RepoError>;
    
    async fn create_job(&self, name: &JobName,
                        check_interval: Duration,
                        lock_ttl: Duration,
                        schedule: Schedule,
                        state: Arc<Mutex<Vec<u8>>>,
                        last_run: DateTime<Utc>,
                        retry_attempts: u32,
                        max_retries: u32,
                        backoff_duration: Duration,) -> Result<(), RepoError>;
}
