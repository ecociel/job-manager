use std::eprintln;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::sync::Mutex;
use crate::{JobMetadata, JobName};
use crate::cassandra::{ErrorKind, RepoError};
use crate::jobs::JobStatus;
use crate::schedule::JobSchedule;

pub mod cassandra;

#[async_trait]
pub trait Repo {
    async fn create_job(&self, name: &JobName, backoff_duration: Duration, check_interval: Duration,
                        last_run: DateTime<Utc>,
                        lock_ttl: Duration,
                        max_retries: u32,
                        retry_attempts: u32,
                        schedule: JobSchedule,
                        state: Arc<Mutex<Vec<u8>>>,
                        status: JobStatus,
                        ) -> Result<(), RepoError>;
    async fn update_lock_ttl(&self, job_name: &str, ttl: Duration) -> Result<(), RepoError>;
    async fn save_and_commit_state(&self, name: &JobName, status: JobStatus,state: Vec<u8>) -> Result<(), RepoError>;
    async fn acquire_lock(&self, name: &str) -> Result<bool,RepoError>;
    async fn release_lock(&self, job_name: &str) -> Result<(), RepoError>;

}
