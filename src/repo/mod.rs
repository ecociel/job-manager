use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::sync::Mutex;
use crate::{JobMetadata, JobName};
use crate::cassandra::RepoError;
use crate::jobs::JobStatus;

pub mod cassandra;

#[async_trait]
pub trait Repo {
    async fn create_job(&self, name: &JobName, backoff_duration: Duration, check_interval: Duration,
                        last_run: DateTime<Utc>,
                        lock_ttl: Duration,
                        max_retries: u32,
                        retry_attempts: u32,
                        schedule: Schedule,
                        state: Arc<Mutex<Vec<u8>>>,
                        status: JobStatus,
                        ) -> Result<(), RepoError>;
    async fn get_job_info(&self, name: &JobName) -> Result<JobMetadata,RepoError>;
    async fn save_and_commit_state(&self, name: &JobName, status: JobStatus) -> Result<(), RepoError>;
    async fn acquire_lock(&self, job_name: &str) -> Result<bool,RepoError>;
    async fn release_lock(&self, job_name: &str) -> Result<(), RepoError>;
}
