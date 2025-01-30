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
            schedule: Schedule::from_str("* * * * * *").unwrap(),
            retry_attempts: 1,
            max_retries: 3,
            backoff_duration: Duration::from_secs(2)
        };

        let job1_func = |state: Vec<u8>| -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> {
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
                    println!("Job 1: HTTP request successful: {:?}", response.text().await);
                } else {
                    println!("Job 1: HTTP request failed with status: {}", response.status());
                }
                Ok(state)
            })
        };
        let job2_cfg = JobCfg {
            name: JobName("job2".to_string()),
            check_interval: Duration::from_secs(10),
            lock_ttl: Duration::from_secs(60),
            schedule: Schedule::from_str("*/10 * * * * *").unwrap(),
            retry_attempts: 1,
            max_retries: 3,
            backoff_duration: Duration::from_secs(2)
        };

        let job2_func = |state: Vec<u8>| -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> {
            Box::pin(async move {
                println!("Job 2: Performing custom task...");
                // tokio::time::sleep(Duration::from_secs(2)).await;
                // println!("Job 2: Task completed!");
                Ok(state)
            })
        };

        manager.register(job1_cfg.clone(), job1_func).await;

        manager.register(job2_cfg.clone(),job1_func).await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        manager.run_registered_jobs(&job1_cfg.name.clone()).await.expect("TODO: panic message");

        manager.run_registered_jobs(&job2_cfg.name.clone()).await.expect("TODO: panic message");


        let scheduler = manager.scheduler.clone();
        let scheduler_lock = scheduler.lock().await;
        scheduler_lock.start().await.unwrap();

        manager.start(job1_func).await.expect("Job 1 run failed");
        manager.start(job2_func).await.expect("Job 2 run failed");
    });
}



// use std::future::Future;
// use std::pin::Pin;
// use cron::Schedule;
// use job::cassandra::TheRepository;
// use job::error::JobError;
// use job::{manager, JobCfg, JobName};
// use std::str::FromStr;
// use std::time::Duration;
// use reqwest::ClientBuilder;
// use tokio::runtime::Runtime;
//
// fn main() {
//     let rt = Runtime::new().expect("Failed to create Tokio runtime");
//     rt.block_on(async {
//         let repo = TheRepository::new(
//             "cassandra".to_string(),
//             Some(("cassandra".to_string(), "cassandra".to_string())),
//         )
//             .await
//             .unwrap();
//
//         let mut manager = manager::Manager::new("job-instance-1".to_string(), repo.clone());
//
//         let job_cfg = JobCfg {
//             name: JobName("job".to_string()),
//             check_interval: Duration::from_secs(5),
//             lock_ttl: Duration::from_secs(60),
//             schedule: Schedule::from_str("*/5 * * * * *").unwrap(),
//         };
//
//         let job_func = |state: Vec<u8>| -> Pin<Box<dyn Future<Output = Result<Vec<u8>, JobError>> + Send>> {
//             Box::pin(async move {
//                 let api_key = "utM6Q3AcxmVPSTTWGqYVSA==8YQTL2X38jLD3k6d";
//                 let client = ClientBuilder::new()
//                     .danger_accept_invalid_certs(true)
//                     .build()
//                     .expect("Failed to create client");
//
//                 let response = client
//                     .get("https://api.api-ninjas.com/v1/interestrate")
//                     .header("X-Api-Key", api_key)
//                     .send()
//                     .await
//                     .map_err(|e| {
//                         JobError::JobExecutionFailed(format!("HTTP request failed: {}", e))
//                     })?;
//
//                 if response.status().is_success() {
//                     println!("HTTP request successful: {:?}", response.text().await);
//                 } else {
//                     println!("HTTP request failed with status: {}", response.status());
//                 }
//                 Ok(state)
//             })
//         };
//
//         match manager.register(job_cfg.name.clone(), job_cfg, move |state| {
//             let job_func = job_func.clone();
//             async move {
//                 job_func(state)
//                     .await
//                     .map_err(|e| anyhow::Error::msg(format!("Job failed: {:?}", e)))
//             }
//         }).await {
//             Ok(_) => {
//                 println!("Job registered successfully");
//             }
//             Err(e) => {
//                 eprintln!("Error registering job: {:?}", e);
//             }
//         }
//         let scheduler = manager.scheduler.clone();
//         let scheduler_lock = scheduler.lock().await;
//         scheduler_lock.start().await.unwrap();
//
//         manager.run(job_func).await.expect("TODO: panic message");
//     });
// }