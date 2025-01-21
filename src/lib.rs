pub mod executor;
pub mod job_manager;
pub mod jobs;
pub mod scheduler;

#[derive(Debug)]
pub struct JobName(pub String);
