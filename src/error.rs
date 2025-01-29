use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use crate::cassandra::RepoError;

pub enum JobError {
    InvalidConfig(String),
    JobInfoNotFound(String),
    SaveStateFailed(String),
    CommitStateFailed(String),
    SchedulerError(String),
    JobExecutionFailed(String),
    RepoError(String),
    GenericError(String),
    DatabaseError(String),
}

impl JobError {
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
            JobError::DatabaseError(msg) => write!(f, "Generic error: {}", msg),
        }
    }
}

impl Debug for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
impl Error for JobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JobError::RepoError(_) => None,
            _ => None,
        }
    }
}

impl From<RepoError> for JobError {
    fn from(error: RepoError) -> Self {
        JobError::RepoError(format!("{}", error))
    }
}

