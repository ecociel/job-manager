mod error;
pub mod executor;
pub mod job_manager;
pub mod jobs;
pub mod scheduler;

#[derive(Debug)]
pub struct JobName(pub String);

impl JobName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
