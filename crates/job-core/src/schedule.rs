use cron::Schedule;
use std::str::FromStr;
use std::fmt::{Debug, Display, Formatter};

#[derive(Clone)]
pub struct JobSchedule(pub(crate) Schedule);


impl Display for JobSchedule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for JobSchedule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for JobSchedule {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobSchedule::new(s)
    }
}

impl From<JobSchedule> for String {
    fn from(value: JobSchedule) -> Self {
        value.0.to_string()
    }
}

impl JobSchedule {
    pub fn new(expression: &str) -> Result<Self, String> {
        Schedule::from_str(expression)
            .map(Self)
            .map_err(|e| format!("Invalid cron expression '{}': {}", expression, e))
    }
    pub fn secondly() -> Self {
        Self::new("* * * * * *").expect("secondly cron expression should parse")
    }
    pub fn minutely() -> Self {
        Self::new("0 * * * * *").expect("minutely cron expression should parse")
    }
    pub fn every_five_minutes() -> Self {
        Self::new("0 */5 * * * *").expect("every_five_minutes cron expression should parse")
    }
}

