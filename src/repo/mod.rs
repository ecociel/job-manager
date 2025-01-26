use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use cassandra_cpp::Cluster;
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::sync::Mutex;
use crate::{JobCfg, JobMetadata, JobName};
use crate::cassandra::ErrorKind::{BindError, ColumnError, ConnectError, DBAuthError, ExecuteError};
use crate::cassandra::{ErrorKind, RepoError};

pub mod cassandra;



#[async_trait]
pub(crate) trait Repo {
    async fn create_job(&self, name: &JobName,
                        backoff_duration: Duration,
                        check_interval: Duration,
                        last_run: DateTime<Utc>,
                        lock_ttl: Duration,
                        max_retries: u32,
                        retry_attempts: u32,
                        schedule: Schedule,
                        state: Arc<Mutex<Vec<u8>>>,
                        ) -> Result<(), RepoError>;
    async fn get_job_info(&self, name: &JobName) -> Result<JobMetadata,RepoError>;

    // async fn get_job_state(&self, name: &JobName) -> Result<Vec<u8>,RepoError>;

    async fn save_and_commit_state(&self, name: &JobName, state: Vec<u8>) -> Result<(), RepoError>;

    //async fn fetch_state(&self, id: String) -> Result<String, RepoError>;
}
