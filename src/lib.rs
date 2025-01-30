pub mod error;
pub mod executor;
pub mod manager;
pub mod jobs;
pub mod repo;
pub mod schedule;

pub use repo::cassandra;

pub use jobs::{JobCfg, JobMetadata};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq,Debug, Clone,Serialize,Deserialize)]
pub struct JobName(pub String);

use std::fmt;

impl fmt::Display for JobName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}


impl JobName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
