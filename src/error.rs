use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum JobError {
    InvalidConfig(String),
    JobInfoNotFound(String),
    SaveStateFailed(String),
    CommitStateFailed(String),
    SchedulerError(String),
    JobExecutionFailed(String),
    RepoError(String),
    GenericError(String),
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobError::InvalidConfig(name) => write!(f, "Invalid job config: {}", name),
            JobError::JobInfoNotFound(name) => write!(f, "Job info not found for job: {}", name),
            JobError::SaveStateFailed(name) => write!(f, "Failed to save state for job: {}", name),
            JobError::CommitStateFailed(name) => {
                write!(f, "Failed to commit state for job: {}", name)
            }
            JobError::SchedulerError(msg) => write!(f, "Scheduler error: {}", msg),
            JobError::JobExecutionFailed(msg) => write!(f, "Job execution failed: {}", msg),
            JobError::RepoError(msg) => write!(f, "Repository error: {}", msg),
            JobError::GenericError(msg) => write!(f, "Generic error: {}", msg),
        }
    }
}
impl Error for JobError {}
