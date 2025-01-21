use async_trait::async_trait;
use chrono::Utc;
use cron::Schedule;
use job::job_manager::{JobCfg, JobsRepo};
use job::jobs::JobMetadata;
use job::{job_manager, JobName};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mock_repo = Arc::new(MockJobsRepo::default());

    let mut manager = job_manager::Manager::new("job-instance-1".to_string(), mock_repo);
    let job_cfg = JobCfg {
        name: JobName("job".to_string()),
        check_interval: Duration::from_secs(5),
        lock_ttl: Duration::from_secs(60),
        schedule: Schedule::from_str("*/5 * * * * *")?,
    };
    manager
        .register("job".to_string(), job_cfg, |state| {
            println!("Executing job with state: {:?}", state);
            Ok(state)
        })
        .await?;

    manager.run().await?;

    tokio::time::sleep(Duration::from_secs(20)).await;

    Ok(())
}

#[derive(Default)]
pub struct MockJobsRepo;

#[async_trait]
impl JobsRepo for MockJobsRepo {
    async fn get_job_info(&self, name: &str) -> Option<JobMetadata> {
        println!("Fetching job info for {}", name);
        Some(JobMetadata {
            name: JobName(name.to_string()),
            check_interval: Duration::from_secs(60),
            lock_ttl: Duration::from_secs(300),
            schedule: Schedule::from_str("*/5 * * * * *").expect(""),
            state: Arc::new(Mutex::new(vec![1, 2, 3])),
            last_run: Utc::now(),
        })
    }

    async fn save_state(&self, name: &str, state: Vec<u8>) -> anyhow::Result<()> {
        println!("Saving state for {}: {:?}", name, state);
        Ok(())
    }

    async fn commit(&self, name: &str, state: Vec<u8>) -> anyhow::Result<()> {
        println!("Committing state for {}: {:?}", name, state);
        Ok(())
    }

    async fn create_job(&self, name: &str, job_cfg: JobCfg) -> anyhow::Result<()> {
        println!("Creating job {} with config: {:?}", name, job_cfg);
        Ok(())
    }
}
