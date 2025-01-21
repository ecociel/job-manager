use crate::error::JobError;
use crate::JobName;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct JobMetadata {
    //TODO: As we proceed and more clear about implmentation we keep adding
    pub name: JobName,
    pub check_interval: Duration,
    pub lock_ttl: Duration,
    pub schedule: Schedule,
    pub state: Arc<Mutex<Vec<u8>>>, //Todo Clarify with Jan
    pub last_run: DateTime<Utc>,
}

#[derive(Debug)]
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

        // Ensure check_interval is at least 1 second
        if self.check_interval < Duration::from_secs(1) {
            return Err(JobError::InvalidConfig(
                "Check interval must be at least 1 second".to_string(),
            ));
        }

        // Ensure lock_ttl is greater than or equal to check_interval
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
    pub(crate) fn due(&self, now: DateTime<Utc>) -> bool {
        let mut upcoming = self.schedule.upcoming(Utc).take(1);
        if let Some(next_run) = upcoming.next() {
            return next_run <= now;
        }
        false
    }
    pub(crate) async fn run<F, Fut>(
        state: &mut Vec<u8>,
        last_run: &mut DateTime<Utc>,
        schedule: &Schedule,
        job_func: F,
    ) -> Result<(), JobError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>, JobError>> + Send + 'static,
    {
        let now = Utc::now();
        if schedule
            .upcoming(Utc)
            .take(1)
            .next()
            .map_or(false, |next_run| next_run <= now)
        {
            let new_state = job_func(state.clone()).await?;
            *state = new_state;
            *last_run = now;

            Ok(())
        } else {
            Err(JobError::GenericError("Job is not due to run".to_string()))
        }
    }
}

pub(crate) fn new<F, Fut>(
    job_cfg: JobCfg,
    job_func: F,
) -> impl FnOnce() -> Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>>
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<Vec<u8>, JobError>> + Send + 'static,
{
    let mut state = Vec::default();
    let mut last_run = Utc::now();
    let mut schedule = job_cfg.schedule.clone();

    move || {
        let job_task = async move {
            JobMetadata::run(&mut state, &mut last_run, &mut schedule, job_func).await
        };

        Box::pin(job_task)
    }
}
