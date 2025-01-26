use std::future::Future;
use std::pin::Pin;
use cron::Schedule;
use job::cassandra::TheRepository;
use job::error::JobError;
use job::{job_manager, JobCfg, JobName};
use std::str::FromStr;
use std::time::Duration;
use reqwest::ClientBuilder;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().expect("Failed to create Tokio runtime");
    rt.block_on(async {
        let repo = TheRepository::new(
            "cassandra".to_string(),
            Some(("cassandra".to_string(), "cassandra".to_string())),
        )
            .await
            .unwrap();

        let mut manager = job_manager::Manager::new("job-instance-1".to_string(), repo.clone());
        eprintln!("Job instance created");

        let job_cfg = JobCfg {
            name: JobName("job".to_string()),
            check_interval: Duration::from_secs(5),
            lock_ttl: Duration::from_secs(60),
            schedule: Schedule::from_str("*/5 * * * * *").unwrap(),
        };

        let job_func = |state: Vec<u8>| -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> {
            Box::pin(async move {
                let api_key = "utM6Q3AcxmVPSTTWGqYVSA==8YQTL2X38jLD3k6d";
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

                if response.status().is_success() {
                    println!("HTTP request successful: {:?}", response.text().await);
                } else {
                    println!("HTTP request failed with status: {}", response.status());
                }
                Ok(state)
            })
        };

        match manager.register(job_cfg.name.clone(), job_cfg, move |state| {
            let job_func = job_func.clone();
            async move {
                job_func(state)
                    .await
                    .map_err(|e| anyhow::Error::msg(format!("Job failed: {:?}", e)))
            }
        }).await {
            Ok(_) => {
                println!("Job registered successfully");
            }
            Err(e) => {
                eprintln!("Error registering job: {:?}", e);
            }
        }

        let scheduler = manager.scheduler.clone();
        let scheduler_lock = scheduler.lock().await;
        scheduler_lock.start().await.unwrap();
        // manager.run().await; TODO// Enable this
        manager.job_executor.start(job_func).await;
    });
}

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     let api_key = "utM6Q3AcxmVPSTTWGqYVSA==8YQTL2X38jLD3k6d";
//         let repo = TheRepository::new(
//         "cassandra".to_string(),
//         Some(("cassandra".to_string(), "cassandra".to_string())),
//     ).await?;
//     let mut manager = job_manager::Manager::new("job-instance-1".to_string(), repo.clone());
//     eprintln!("Job instance created");
//     let job_cfg = JobCfg {
//         name: JobName("job".to_string()),
//         check_interval: Duration::from_secs(5),
//         lock_ttl: Duration::from_secs(60),
//         schedule: Schedule::from_str("*/5 * * * * *")?,
//     };
//     manager.register(job_cfg.name.clone(), job_cfg, move |state| {
//         eprintln!("Job Registered with state {:?}", state);
//             // let repo = repo_clone.clone();
//             async move {
//                 eprintln!("Performing HTTP request...");
//                 let client = Client::new();
//                 let response = client.get("https://api.api-ninjas.com/v1/interestrate")
//                     .header("X-Api-Key", api_key)
//                     .send()
//                     .await
//                     .map_err(|e| JobError::JobExecutionFailed(format!("HTTP request failed: {}", e)))?;
//
//                 if response.status().is_success() {
//                     println!("HTTP request successful: {:?}", response.text().await?);
//                 } else {
//                     println!("HTTP request failed with status: {}", response.status());
//                 }
//
//                 eprintln!("Job completed successfully with state: {:?}", state);
//                 Ok(state)
//             }
//         })
//         .await?;
//
//     eprintln!("Starting job manager...");
//     manager.run().await?;
//     tokio::time::sleep(Duration::from_secs(20)).await;
//
//     Ok(())
// }
