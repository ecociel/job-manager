use crate::repo::cassandra::ErrorKind::{
    BindError, ColumnError, ConnectError, CreateKeySpaceError, CreateTableError, DBAuthError,
    ExecuteError, InvalidTimeStamp,
};
use cassandra_cpp::{AsRustType, BindRustType, Cluster, LendingIterator, Session};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use crate::{JobCfg, JobMetadata, JobName};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use crate::cassandra::ErrorKind::RowAlreadyExists;
use crate::repo::Repo;

#[derive(Clone, Debug)]
pub struct TheRepository {
    session: Session,
}

impl TheRepository {
    pub async fn new(
        uri: String,
        credentials: Option<(String, String)>,
    ) -> Result<Self, RepoError> {
        let (user, password) = credentials.ok_or_else(|| RepoError {
            target: uri.clone(),
            kind: ErrorKind::InvalidCredentialsError,
        })?;

        let mut cluster = Cluster::default();

        cluster
            .set_credentials(user.as_str(), password.as_str())
            .map_err(|e| RepoError {
                target: uri.clone(),
                kind: DBAuthError(e.into()),
            })?;

        cluster
            .set_contact_points(uri.as_str())
            .map_err(|e| RepoError {
                target: uri.clone(),
                kind: ConnectError(e.into()),
            })?;

        let session = cluster.connect().await.map_err(|e| RepoError {
            target: uri.clone(),
            kind: ConnectError(e.into()),
        })?;

        Ok(Self { session })
    }

}

#[async_trait]
impl Repo for TheRepository {
    async fn create_job(
        &self,
        name: &JobName,
        backoff_duration: Duration,
        check_interval: Duration,
        last_run: DateTime<Utc>,
        lock_ttl: Duration,
        retry_attempts: u32,
        max_retries: u32,
        schedule: Schedule,
        state: Arc<Mutex<Vec<u8>>>,
    ) -> Result<(), RepoError> {
        let mut statement = self
            .session
            .statement("INSERT INTO job.jobs (name, backoff_duration, check_interval, last_run, lock_ttl,max_retries, retry_attempts,schedule, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

        statement.bind(0, name.0.as_str()).map_err(|e| RepoError {
            target: name.0.clone(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        statement.bind(1, backoff_duration.as_secs() as i64).map_err(|e| RepoError {
            target: "backoff_duration".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        statement.bind(2, check_interval.as_secs() as i32).map_err(|e| RepoError {
            target: "check_interval".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        let last_run_epoch = last_run.naive_utc().and_utc().timestamp();
        statement
            .bind(3, last_run_epoch)
            .map_err(|e| RepoError {
                target: "last_run".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;
        statement.bind(4, lock_ttl.as_secs() as i32).map_err(|e| RepoError {
            target: "lock_ttl".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        statement
            .bind(5, max_retries as i32)
            .map_err(|e| RepoError {
                target: "max_retries".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;
        statement.bind(6, retry_attempts as i32).map_err(|e| RepoError {
            target: "retry_attempts".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        statement.bind(7,schedule.to_string().as_str()).map_err(|e| RepoError {
            target: "schedule".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        let state_bytes = state.lock().await.clone();
        statement
            .bind(8, state_bytes)
            .map_err(|e| RepoError {
                target: "state".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;
        let result =statement.execute().await.map_err(|e| RepoError {
            target: "job.jobs".to_string(),
            kind: ErrorKind::ExecuteError(e.into()),
        })?;

        if let Some(row) = result.first_row() {
            let app: bool = row.get_by_name("[applied]").map_err(|e| RepoError{
                target: "jobs - [applied] status".to_string(),
                kind: ExecuteError(e.into()),
            })?;

            if !app {
                return Err(RepoError {
                    target: name.0.clone(),
                    kind: RowAlreadyExists
                });
            }
        }

        Ok(())
    }


    async fn commit(&self, name: &JobName, state: Vec<u8>) -> Result<(), RepoError> {
        let mut statement = self.session.statement(
            "UPDATE job_states SET state = ?, committed = true WHERE job_name = ?;",
        );

        statement.bind(0, state).map_err(|e| {
            RepoError {
                target: "state".to_string(),
                kind: ErrorKind::BindError(e.into()),
            }
        })?;
        statement.bind(1, name.0.as_str()).map_err(|e| {
            RepoError {
                target: name.0.clone(),
                kind: ErrorKind::BindError(e.into()),
            }
        })?;

        let result = statement.execute().await.map_err(|e| {
            RepoError {
                target: "job_states".to_string(),
                kind: ErrorKind::ExecuteError(e.into()),
            }
        })?;

        if result.first_row().is_none() {
            return Err(RepoError {
                target: name.0.clone(),
                kind: ErrorKind::RowAlreadyExists,
            });
        }

        Ok(())
    }

    async fn get_job_info(&self, name: &JobName) -> Result<JobMetadata, RepoError> {
        let query = "SELECT check_interval, lock_ttl, schedule, state, last_run, retry_attempts, max_retries, backoff_duration FROM jobs WHERE name = ?;";
        let mut statement = self.session.statement(query);

        statement.bind(0, name.0.as_str()).map_err(|e| RepoError {
            target: name.0.clone(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let result = statement.execute().await.map_err(|e| RepoError {
            target: "jobs".to_string(),
            kind: ErrorKind::ExecuteError(e.into()),
        })?;

        if let Some(row) = result.first_row() {
            let check_interval_secs: i64 = row.get_by_name("check_interval").unwrap();
            let lock_ttl_secs: i64 = row.get_by_name("lock_ttl").unwrap();
            let schedule_str: String = row.get_by_name("schedule").unwrap();
            let state_bytes: Vec<u8> = row.get_by_name("state").unwrap();
            let last_run_str: String = row.get_by_name("last_run").unwrap();
            let retry_attempts: i32 = row.get_by_name("retry_attempts").unwrap();
            let max_retries: i32 = row.get_by_name("max_retries").unwrap();
            let backoff_duration_secs: i64 = row.get_by_name("backoff_duration").unwrap();
            let check_interval = Duration::from_secs(check_interval_secs as u64);
            let lock_ttl = Duration::from_secs(lock_ttl_secs as u64);
            let schedule = Schedule::from_str(&schedule_str).map_err(|_| {
                RepoError {
                    target: "schedule".to_string(),
                    kind: ErrorKind::InvalidConfig("Schedule parse error".to_string()),
                }
            })?;
            let state = Arc::new(Mutex::new(state_bytes));
            let last_run = DateTime::parse_from_rfc3339(&last_run_str)
                .map_err(|e| RepoError {
                    target: "last_run".to_string(),
                    kind: ErrorKind::InvalidConfig("Last run parse error".to_string()),
                })?
                .with_timezone(&Utc);
            let backoff_duration = Duration::from_secs(backoff_duration_secs as u64);

            Ok(JobMetadata {
                name: name.clone(),
                check_interval,
                lock_ttl,
                schedule,
                state,
                last_run,
                retry_attempts: retry_attempts as u32,
                max_retries: max_retries as u32,
                backoff_duration,
            })
        } else {
            Err(RepoError {
                target: name.0.clone(),
                kind: ErrorKind::NotFound,
            })
        }
    }


    async fn save_state(&self, id: String, state: Vec<u8>) -> Result<(), RepoError> {
        let mut statement = self
            .session
            .statement("INSERT INTO icp_device.state (id, state) VALUES (?, ?) IF NOT EXISTS;");

        statement.bind(0, id.as_str()).map_err(|e| RepoError {
            target: "id".to_string(),
            kind: BindError(e.into()),
        })?;
        statement.bind(1, state).map_err(|e| RepoError {
            target: "state".to_string(),
            kind: BindError(e.into()),
        })?;

        let result = statement.execute().await.map_err(|e| RepoError {
            target: "icp_device.state".to_string(),
            kind: ExecuteError(e.into()),
        })?;

        if let Some(row) = result.first_row() {
            let app: bool = row.get_by_name("[applied]").map_err(|e| RepoError {
                target: "state - [applied] status".to_string(),
                kind: ExecuteError(e.into()),
            })?;

            if !app {
                return Err(RepoError {
                    target: id.clone(),
                    kind: ErrorKind::RowAlreadyExists,
                });
            }
        }

        Ok(())
    }

    async fn fetch_state(&self, id: String) -> Result<String, RepoError> {
        let query = "SELECT state FROM icp_device.state WHERE id = ?;";
        let mut statement = self.session.statement(query);

        statement.bind_string(0, &id).map_err(|e| RepoError {
            target: "id".to_string(),
            kind: BindError(e.into()),
        })?;

        let result = statement.execute().await.map_err(|e| RepoError {
            target: "icp_device.state".to_string(),
            kind: ExecuteError(e.into()),
        })?;

        match result.first_row() {
            None => Err(RepoError {
                target: id.clone(),
                kind: ErrorKind::RowAlreadyExists,
            }),
            Some(row) => {
                let state: String = row.get_by_name("state").map_err(|e| RepoError {
                    target: "state".to_string(),
                    kind: ColumnError(e.into()),
                })?;
                Ok(state)
            }
        }
    }

}

#[derive(Debug)]
pub enum LoadError {
    InnerDBError(String),
    InvalidTimestamp(String),
}

impl Display for LoadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::InnerDBError(msg) => write!(f, "Cassandra error: {}", msg),
            LoadError::InvalidTimestamp(msg) => write!(f, "Invalid timestamp: {}", msg),
        }
    }
}

impl Error for LoadError {}

impl From<cassandra_cpp::Error> for LoadError {
    fn from(error: cassandra_cpp::Error) -> Self {
        LoadError::InnerDBError(error.to_string())
    }
}

#[derive(Debug)]
pub struct CassandraErrorKind(String);

impl From<cassandra_cpp::Error> for CassandraErrorKind {
    fn from(error: cassandra_cpp::Error) -> Self {
        CassandraErrorKind(format!("{}", error))
    }
}


pub struct RepoError {
    target: String,
    kind: ErrorKind,
}

#[derive(Debug)]
pub enum ErrorKind {
    ConnectError(CassandraErrorKind),
    DBAuthError(CassandraErrorKind),
    InvalidCredentialsError,
    InvalidTimeStamp,
    CreateKeySpaceError(CassandraErrorKind),
    CreateTableError(CassandraErrorKind),
    BindError(CassandraErrorKind),
    ExecuteError(CassandraErrorKind),
    ColumnError(CassandraErrorKind),
    RowAlreadyExists,
    InvalidConfig(String),
    NotFound,
}


impl Display for RepoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ConnectError(err) => write!(
                f,
                "Failed to connect or initialize repo for database - {}: {}",
                self.target, err
            ),
            DBAuthError(err) => write!(
                f,
                "Authentication/credentials error for database - {}: {}",
                self.target, err
            ),
            ErrorKind::InvalidCredentialsError => write!(
                f,
                "Invalid credentials provided for database - {}",
                self.target
            ),
            CreateKeySpaceError(err) => write!(f, "Error creating keyspace: {}", err),
            CreateTableError(err) => write!(f, "Error creating table: {}", err),
            BindError(err) => write!(f, "error in binding type:{}-{}", self.target, err),
            ExecuteError(err) => write!(f, "error in executing query:{}-{}", self.target, err),
            ColumnError(err) => write!(f, "error in fetching column:{}-{}", self.target, err),
            InvalidTimeStamp => write!(f, "Invalid timestamp:{}", self.target),
            ErrorKind::RowAlreadyExists => write!(
                f,
                "Row with the given ID already exists in the database: {}",
                self.target
            ),
            ErrorKind::InvalidConfig(msg) => write!(
                f,
                "Invalid configuration for target: {}. Reason: {}",
                self.target, msg
            ),
            ErrorKind::NotFound => write!(
                f,
                "The requested item was not found in the database: {}",
                self.target
            ),
        }
    }
}

impl Debug for RepoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepoError {{ target: {}, kind: {:?} }}",
            self.target, self.kind
        )
    }
}

impl Error for RepoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl Display for CassandraErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for CassandraErrorKind {}
