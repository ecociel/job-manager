use cron::Schedule;
use job::cassandra::TheRepository;
use job::error::JobError;
use job::{job_manager, JobCfg, JobName};
use std::str::FromStr;
use std::time::Duration;
use reqwest::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
        let repo = TheRepository::new(
        "cassandra".to_string(),
        Some(("cassandra".to_string(), "cassandra".to_string())),
    ).await?;
    let mut manager = job_manager::Manager::new("job-instance-1".to_string(), repo.clone());
    eprintln!("Job instance created");
    let job_cfg = JobCfg {
        name: JobName("job".to_string()),
        check_interval: Duration::from_secs(5),
        lock_ttl: Duration::from_secs(60),
        schedule: Schedule::from_str("* * * * * *")?,
    };
    eprintln!("Job config {:?}", job_cfg);
    let repo_clone = repo.clone();
    manager.register(job_cfg.name.clone(), job_cfg, move |state| {
        eprintln!("Job Registered with state {:?}", state);
            // let repo = repo_clone.clone();
            async move {
                let client = Client::new();
                let response = client.get("http://worldtimeapi.org/api/timezone/Europe/London.txt")
                    .send()
                    .await
                    .map_err(|e| JobError::JobExecutionFailed(format!("HTTP request failed: {}", e)))?;

                if response.status().is_success() {
                    println!("HTTP request successful: {:?}", response.text().await?);
                } else {
                    println!("HTTP request failed with status: {}", response.status());
                }

                // Fetch the previous state from Cassandra
                // if let Some(previous_state) = repo.fetch_state("job".to_string()).await? {
                //     println!("Previous state: {:?}", previous_state);
                // } else {
                //     println!("No previous state found for this job.");
                // }

                // Save the new state to Cassandra

                // repo.save_state("job", &format!("{:?}", state))
                //     .await
                //     .map_err(|e| JobError::SaveStateFailed(e.to_string()))?;
                Ok(state)
            }
        })
        .await?;

    manager.run().await?;
    tokio::time::sleep(Duration::from_secs(20)).await;

    Ok(())
}
