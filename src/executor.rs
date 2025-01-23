use crate::scheduler::Scheduler;
use crate::JobMetadata;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub struct JobExecutor {
    scheduler: Arc<Mutex<Scheduler>>,
    jobs: Arc<Mutex<Vec<JobMetadata>>>,
}

impl JobExecutor {
    pub fn new(scheduler: Arc<Mutex<Scheduler>>) -> Self {
        JobExecutor {
            scheduler,
            jobs: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn add_job(&self, job_metadata: JobMetadata) {
        let mut jobs = self.jobs.lock().await;
        jobs.push(job_metadata);
    }

    pub async fn start(&self) {
        loop {
            let now = Utc::now();

            let jobs = self.jobs.lock().await.clone();

            for job in jobs.iter() {
                if job.due(now) {
                    let job_func = move |state: Vec<u8>| async { Ok(state) };
                    let mut job_clone = job.clone();
                    let state_clone = job.state.clone();
                    tokio::spawn(async move {
                        let state_lock = state_clone.lock().await;
                        let mut state = state_lock.clone();

                        let schedule = job_clone.schedule.clone();
                        let mut last_run = job_clone.last_run;
                        let result = job_clone
                            .run(&mut state, &mut last_run, &schedule, job_func)
                            .await;

                        if let Err(err) = result {
                            eprintln!("Error executing job {:?}: {}", job_clone.name, err);
                        } else {
                            eprintln!("Job {:?} completed successfully", job_clone.name);
                        }
                    });
                }
            }
            sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
