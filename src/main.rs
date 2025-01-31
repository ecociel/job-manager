use std::env;
use std::future::Future;
use std::pin::Pin;
use cron::Schedule;
use job::cassandra::TheRepository;
use job::error::JobError;
use job::{manager, JobCfg, JobName};
use std::str::FromStr;
use std::time::Duration;
use log::info;
use reqwest::ClientBuilder;
use tokio::runtime::Runtime;
use job::schedule::JobSchedule;

fn main() {
    let rt = Runtime::new().expect("Failed to create Tokio runtime");
    rt.block_on(async {
        let repo = TheRepository::new(
            "cassandra".to_string(),
            Some(("cassandra".to_string(), "cassandra".to_string())),
        )
            .await
            .unwrap();
        let mut manager = manager::Manager::new("job-instance-1".to_string(), repo.clone());

        let job1_cfg = JobCfg {
            name: JobName("job1".to_string()),
            check_interval: Duration::from_secs(5),
            lock_ttl: Duration::from_secs(60),
            schedule: JobSchedule::secondly(),
            retry_attempts: 1,
            max_retries: 3,
            backoff_duration: Duration::from_secs(2)
        };

        let job1_func = |state: Vec<u8>| -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> {
            Box::pin(async move {
                let api_key = match env::var("API_KEY") {
                    Ok(val) => val,
                    Err(_) => {
                        eprintln!("API_KEY environment variable not set.");
                        return Err(JobError::JobExecutionFailed("Missing API_KEY".to_string()));
                    }
                };
                let client = ClientBuilder::new()
                    .danger_accept_invalid_certs(true)
                    .build()
                    .expect("Failed to create client");

                let response = client
                    .get("https://api.api-ninjas.com/v1/interestrate")
                    .header("X-Api-Key", api_key)
                    .send()
                    .await
                    .map_err(|e| {
                        JobError::JobExecutionFailed(format!("HTTP request failed: {}", e))
                    })?;
                let mut new_state = state.clone();
                if response.status().is_success() {
                    println!("Job 1: HTTP request successful with status: {}", response.status());
                    new_state = b"200 OK".to_vec();
                } else {
                    println!("Job 1: HTTP request failed with status: {}", response.status());
                    new_state = b"Failed".to_vec();
                }

                Ok(new_state)
            })
        };
        manager.register(job1_cfg.clone(), job1_func).await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        manager.run(&job1_cfg.name.clone(), job1_cfg.clone(), job1_func).await.expect("TODO: panic message");

        manager.start(job1_func).await.expect("Job 1 run failed");

    });
}

