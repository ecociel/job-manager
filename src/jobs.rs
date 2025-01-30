use crate::error::JobError;
use crate::JobName;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use derive_more::Display;
use log::warn;
use crate::schedule::JobSchedule;

#[derive(Clone, Debug)]
pub struct JobMetadata {
    //TODO: As we proceed and more clear about implmentation we keep adding
    pub name: JobName,
    pub state: Arc<Mutex<Vec<u8>>>,
    pub last_run: DateTime<Utc>,
    pub status: JobStatus,
    pub check_interval: Duration,
    pub lock_ttl: Duration,
    pub schedule: JobSchedule,
    pub retry_attempts: u32,
    pub max_retries: u32,
    pub backoff_duration: Duration,
}
#[derive(Debug, Clone, PartialEq,Display)]
pub enum JobStatus {
    #[display(fmt = "initializing")]
    Initializing,
    #[display(fmt = "running")]
    Running,
    #[display(fmt = "retrying")]
    Retrying,
    #[display(fmt = "failed")]
    Failed,
    #[display(fmt = "completed")]
    Completed,
}

impl JobStatus {
    pub fn as_string(&self) -> String {
        match self {
            JobStatus::Initializing => "initializing".to_string(),
            JobStatus::Running => "running".to_string(),
            JobStatus::Retrying => "retrying".to_string(),
            JobStatus::Failed => "failed".to_string(),
            JobStatus::Completed => "completed".to_string(),
        }
    }

    pub fn from_string(status: &str) -> Self {
        match status {
            "running" => JobStatus::Running,
            "retrying" => JobStatus::Retrying,
            "failed" => JobStatus::Failed,
            "completed" => JobStatus::Completed,
            _ => JobStatus::Initializing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobCfg {
    pub name: JobName,
    pub check_interval: Duration,
    pub lock_ttl: Duration,
    pub schedule: JobSchedule,
    pub retry_attempts: u32,
    pub max_retries: u32,
    pub backoff_duration: Duration,
}

impl JobCfg {
    //TODO: Add Cron validation
    pub fn validate(&self) -> Result<(), JobError> {
        if self.name.as_str().trim().is_empty() {
            return Err(JobError::InvalidConfig(
                "Job name cannot be empty.".to_string(),
            ));
        }
        if self.check_interval < Duration::from_secs(1) {
            return Err(JobError::InvalidConfig(
                "Check interval must be at least 1 second".to_string(),
            ));
        }
        if self.lock_ttl < self.check_interval {
            return Err(JobError::InvalidConfig(
                "Lock TTL must be greater than or equal to the check interval".to_string(),
            ));
        }
        if self.retry_attempts > self.max_retries {
            return Err(JobError::InvalidConfig(
                "Retry attempts cannot exceed max retries.".to_string(),
            ));
        }

        if self.max_retries == 0 {
            return Err(JobError::InvalidConfig(
                "Max retries must be at least 1.".to_string(),
            ));
        }

        if self.backoff_duration < Duration::from_millis(100) {
            return Err(JobError::InvalidConfig(
                "Backoff duration must be at least 100 milliseconds.".to_string(),
            ));
        }

        let schedule_str = self.schedule.to_string();
        if Schedule::from_str(&schedule_str).is_err() {
            return Err(JobError::InvalidConfig("Schedule is invalid.".to_string()));
        }

        Ok(())
    }
}

impl JobMetadata {
    pub fn due(&self, now: DateTime<Utc>) -> bool {
        let mut upcoming = self.schedule.0.after(&now);
        if let Some(next_run) = upcoming.next() {
            let tolerance = 1;
            return (next_run - now).num_seconds().abs() <= tolerance;
        }
        false
    }
    pub async fn run<F, Fut>(
        &mut self,
        state: &mut Vec<u8>,
        last_run: &mut DateTime<Utc>,
        schedule: &JobSchedule,
        job_func: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=Result<Vec<u8>, JobError>> + Send + 'static,
    {
        loop {
            let now = Utc::now();
            let mut upcoming = schedule.0.after(&now);

            if let Some(run_time) = upcoming.next() {
                if now < run_time {
                    let wait_time = (run_time - now).to_std().unwrap_or(Duration::from_secs(1));
                    tokio::time::sleep(wait_time).await;
                }
            }

            let mut attempt = 0;
            loop {
                let new_state = job_func(state.clone()).await;

                match new_state {
                    Ok(new_state) => {
                        *state = new_state;
                        *last_run = Utc::now();
                        return Ok(());
                    }
                    Err(e) if attempt < self.max_retries => {
                        attempt += 1;
                        warn!(
                        "Job failed, retrying {}/{}: {}",
                        attempt, self.max_retries, e
                    );
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Job failed after {} attempts: {}", self.max_retries, e));
                    }
                }
            }
        }
    }
}
