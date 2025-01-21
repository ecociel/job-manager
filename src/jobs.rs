use crate::job_manager::JobCfg;
use crate::JobName;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::future::Future;
use std::pin::Pin;
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

impl JobMetadata {
    pub(crate) fn due(&self, now: DateTime<Utc>) -> bool {
        let mut upcoming = self.schedule.upcoming(Utc).take(1);
        if let Some(next_run) = upcoming.next() {
            return next_run <= now;
        }
        false
    }
    pub(crate) async fn run<F, Fut>(
        state: &mut Vec<u8>,          // Mutable reference to state
        last_run: &mut DateTime<Utc>, // Mutable reference to last_run
        schedule: &Schedule,
        job_func: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
    {
        let now = Utc::now();
        if schedule
            .upcoming(Utc)
            .take(1)
            .next()
            .map_or(false, |next_run| next_run <= now)
        {
            // Clone the current state to pass into the job function
            let new_state = job_func(state.clone()).await?;

            // Update the state
            *state = new_state;

            // Update the last run time
            *last_run = now;

            Ok(())
        } else {
            Err(anyhow::anyhow!("Job is not due to run").into())
        }
    }
}

pub(crate) fn new<F, Fut>(
    job_cfg: JobCfg,
    job_func: F,
) -> impl FnOnce() -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
{
    // Initialize job metadata
    let mut state = Vec::default(); // Local mutable state
    let mut last_run = Utc::now(); // Local mutable last_run
    let mut schedule = job_cfg.schedule.clone();

    // Create a closure to run the job
    move || {
        let job_task = async move {
            JobMetadata::run(&mut state, &mut last_run, &mut schedule, job_func).await
        };

        Box::pin(job_task)
    }
}
