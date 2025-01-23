use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;

pub struct Scheduler {
    jobs:
        Mutex<Vec<Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            jobs: Mutex::new(Vec::new()),
        }
    }

    pub async fn add<F>(&self, job: F) -> Result<()>
    where
        F: Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
    {
        let mut jobs = self.jobs.lock().await;
        jobs.push(Box::new(job));
        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        let jobs = self.jobs.lock().await;
        for job in jobs.iter() {
            job();
        }
        Ok(())
    }
}
