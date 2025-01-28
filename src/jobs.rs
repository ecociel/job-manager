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

#[derive(Clone, Debug)]
pub struct JobMetadata {
    //TODO: As we proceed and more clear about implmentation we keep adding
    pub name: JobName,
    pub check_interval: Duration,
    pub lock_ttl: Duration,
    pub schedule: Schedule,
    pub state: Arc<Mutex<Vec<u8>>>, //Todo Clarify with Jan
    pub last_run: DateTime<Utc>,
    pub retry_attempts: u32,
    pub max_retries: u32,
    pub backoff_duration: Duration,
}
#[derive(Debug, Clone, PartialEq)]
pub enum JobState {
    Initializing,
    Running,
    Retrying,
    Failed,
    Completed,
}

impl JobState {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            JobState::Initializing => b"initializing".to_vec(),
            JobState::Running => b"running".to_vec(),
            JobState::Retrying => b"retrying".to_vec(),
            JobState::Failed => b"failed".to_vec(),
            JobState::Completed => b"completed".to_vec(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        match bytes {
            b"running" => JobState::Running,
            b"retrying" => JobState::Retrying,
            b"failed" => JobState::Failed,
            b"completed" => JobState::Completed,
            _ => JobState::Initializing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobCfg {
    pub name: JobName,
    pub check_interval: Duration,
    pub lock_ttl: Duration,
    pub schedule: Schedule,
}

impl JobCfg {
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

        let schedule_str = self.schedule.to_string();
        if Schedule::from_str(&schedule_str).is_err() {
            return Err(JobError::InvalidConfig("Schedule is invalid.".to_string()));
        }

        Ok(())
    }
}

impl JobMetadata {
    pub fn due(&self, now: DateTime<Utc>) -> bool {
        let mut upcoming = self.schedule.upcoming(Utc).take(1);
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
        _schedule: &Schedule, // TODO Not being used here Fix it!!
        job_func: F,
    ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let now = Utc::now();
            let mut attempt = 0;
            loop {
                let new_state = job_func(state.clone()).await;

                match new_state {
                    Ok(new_state) => {
                        *state = new_state;
                        *last_run = now;
                        return Ok(());
                    }
                    Err(_e) if attempt < self.max_retries => { //TODO Fix the error!!
                        attempt += 1;
                        eprintln!("Job failed, retrying {}/{}", attempt, self.max_retries);
                        sleep(self.backoff_duration).await;
                    }
                    Err(e) => {
                        eprintln!("Job failed after {} attempts", self.max_retries);
                        return Err(JobError::JobExecutionFailed(e.to_string()));
                    }
                }
            }
    }
}

// pub(crate) fn new<F, Fut>(
//     job_cfg: JobCfg,
//     job_func: F,
// ) -> impl FnOnce() -> Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>>
// where
//     F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
//     Fut: Future<Output = anyhow::Result<Vec<u8>, JobError>> + Send + 'static,
// {
//     let mut state = Vec::default();
//     let mut last_run = Utc::now();
//     let mut schedule = job_cfg.schedule.clone();
//     let mut job_metadata = JobMetadata {
//         name: job_cfg.name,
//         check_interval: job_cfg.check_interval,
//         lock_ttl: job_cfg.lock_ttl,
//         schedule: job_cfg.schedule.clone(),
//         state: Arc::new(Mutex::new(state.clone())),
//         last_run,
//         retry_attempts: 0,
//         max_retries: 3,
//         backoff_duration: Duration::from_secs(2),
//     };
//     move || {
//         let job_task = async move {
//             JobMetadata::run(
//                 &mut job_metadata,
//                 &mut state,
//                 &mut last_run,
//                 &mut schedule,
//                 job_func,
//             )
//             .await
//         };
//         Box::pin(job_task)
//     }
// }
