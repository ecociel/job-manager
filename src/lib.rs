pub mod error;
pub mod executor;
pub mod job_manager;
pub mod jobs;
mod repo;
pub mod scheduler;

pub use repo::cassandra;

pub use jobs::{JobCfg, JobMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone,Serialize,Deserialize)]
pub struct JobName(pub String);

impl JobName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
