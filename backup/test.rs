use async_trait::async_trait;
use chrono::Utc;
use cron::Schedule;
use job::error::JobError;
use job::job_manager::{JobsRepo, Manager};
use job::jobs::{JobCfg, JobMetadata};
use job::{job_manager, JobName};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::time::Duration;

#[derive(Clone)]
pub struct MockDb {
    data: Arc<Mutex<Vec<u8>>>,
}

impl MockDb {
    pub fn new() -> Self {
        MockDb {
            data: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn save(&self, name: &JobName, state: &[u8]) -> anyhow::Result<()> {
        let mut db = self.data.lock().unwrap();
        db.clear();
        db.extend_from_slice(state);
        println!("Saved state for job '{}': {:?}", name.as_str(), state);
        Ok(())
    }

    pub async fn fetch(&self) -> Vec<u8> {
        let db = self.data.lock().unwrap();
        db.clone()
    }
}

#[derive(Clone)]
pub struct MockJobsRepo {
    db: MockDb,
}

impl MockJobsRepo {
    pub fn new(db: MockDb) -> Self {
        MockJobsRepo { db }
    }
}

#[async_trait]
impl JobsRepo for MockJobsRepo {
    async fn get_job_info(&self, name: &JobName) -> Option<JobMetadata> {
        println!("Fetching job info for {}", name.as_str());
        Some(JobMetadata {
            name: name.clone(),
            check_interval: Duration::from_secs(60),
            lock_ttl: Duration::from_secs(300),
            schedule: Schedule::from_str("*/5 * * * * *").expect("Invalid schedule"),
            state: Arc::new(tokio::sync::Mutex::new(vec![1, 2, 3])),
            last_run: Utc::now(),
            retry_attempts: 0,
            max_retries: 3,
            backoff_duration: Duration::from_secs(2),
        })
    }

    async fn save_state(&self, name: &JobName, state: Vec<u8>) -> anyhow::Result<(), JobError> {
        self.db
            .save(name, &state)
            .await
            .map_err(|e| JobError::SaveStateFailed(e.to_string()))?;
        Ok(())
    }

    async fn commit(&self, name: &JobName, state: Vec<u8>) -> anyhow::Result<(), JobError> {
        println!("Committing state for {}: {:?}", name.as_str(), state);
        Ok(())
    }

    async fn create_job(&self, name: &JobName, job_cfg: JobCfg) -> anyhow::Result<(), JobError> {
        println!("Creating job {} with config: {:?}", name.as_str(), job_cfg);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = MockDb::new();
    let mock_repo = Arc::new(MockJobsRepo::new(db.clone()));

    let mut manager = job_manager::Manager::new("job-instance-1".to_string(), mock_repo.clone());

    let job_cfg = JobCfg {
        name: JobName("job".to_string()),
        check_interval: Duration::from_secs(5),
        lock_ttl: Duration::from_secs(60),
        schedule: Schedule::from_str("*/5 * * * * *")?,
    };

    let job_repo = mock_repo.clone();
    manager
        .register(job_cfg.name.clone(), job_cfg, move |state| {
            let job_repo = job_repo.clone();
            async move {
                println!("Executing job with state: {:?}", state);

                let fetched_state = job_repo.db.fetch().await;
                println!("Fetched previous state: {:?}", fetched_state);

                job_repo
                    .save_state(&JobName("job".to_string()), state.clone())
                    .await
                    .map_err(|e| JobError::SaveStateFailed(e.to_string()))?;

                Ok(state)
            }
        })
        .await?;
    manager.run().await?;
    tokio::time::sleep(Duration::from_secs(20)).await;

    Ok(())
}

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     let db = MockDb::new();
//     let mock_repo = Arc::new(MockJobsRepo::new(db.clone()));
//
//     let mut manager = Manager::new("job-instance-1".to_string(), mock_repo.clone());
//
//     let job_cfg = JobCfg {
//         name: JobName("job".to_string()),
//         check_interval: Duration::from_secs(5),
//         lock_ttl: Duration::from_secs(60),
//         schedule: Schedule::from_str("*/5 * * * * *")?,
//     };
//
//     let job_repo = mock_repo.clone();
//     manager
//         .register(JobName("job".to_string()), job_cfg, move |state| {
//             let job_repo = job_repo.clone();
//             async move {
//                 println!("Executing job with state: {:?}", state);
//
//                 let fetched_state = job_repo.db.fetch().await;
//                 println!("Fetched previous state: {:?}", fetched_state);
//
//                 job_repo
//                     .save_state(&JobName("job".to_string()), state.clone())
//                     .await?;
//
//                 Ok(state)
//             }
//         })
//         .await?;
//
//     manager.run().await?;
//     tokio::time::sleep(Duration::from_secs(20)).await;
//
//     Ok(())
// }
