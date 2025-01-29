use crate::repo::cassandra::ErrorKind::{
    BindError, ColumnError, ConnectError, CreateKeySpaceError, CreateTableError, DBAuthError,
    ExecuteError, InvalidTimeStamp,
};
use cassandra_cpp::{AsRustType, BindRustType, Cluster, Session};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use crate::{JobMetadata, JobName};
use tokio::sync::Mutex;
use crate::cassandra::ErrorKind::RowAlreadyExists;
use crate::repo::Repo;
use gethostname::gethostname;
use crate::jobs::JobStatus;

#[derive(Clone, Debug)]
pub struct TheRepository {
    session: Session,
    hostname: String,
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
        let hostname = gethostname().into_string().unwrap_or_else(|_| "unknown-host".to_string());

        Ok(Self { session, hostname })
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
        status: JobStatus,
    ) -> Result<(), RepoError> {
        let mut statement = self
            .session
            .statement("INSERT INTO job.jobs (name, backoff_duration, check_interval, last_run, lock_ttl, max_retries, retry_attempts, schedule, state, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

        statement.bind(0, name.0.as_str()).map_err(|e| RepoError {
            target: name.0.clone(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let backoff_secs = backoff_duration.as_secs() as i64;
        statement.bind(1, backoff_secs).map_err(|e| RepoError {
            target: "backoff_duration".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let check_interval_secs = check_interval.as_secs() as i64;
        statement.bind(2, check_interval_secs).map_err(|e| RepoError {
            target: "check_interval".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let last_run_epoch = last_run.naive_utc().and_utc().timestamp();
        statement.bind(3, last_run_epoch).map_err(|e| RepoError {
            target: "last_run".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let lock_ttl_secs = lock_ttl.as_secs() as i32;
        statement.bind(4, lock_ttl_secs).map_err(|e| RepoError {
            target: "lock_ttl".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        statement.bind(5, max_retries as i32).map_err(|e| RepoError {
            target: "max_retries".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        statement.bind(6, retry_attempts as i32).map_err(|e| RepoError {
            target: "retry_attempts".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;
        statement.bind(7, schedule.to_string().as_str()).map_err(|e| RepoError {
            target: "schedule".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let state_bytes = state.lock().await.clone();
        statement.bind(8, state_bytes).map_err(|e| RepoError {
            target: "state".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        statement.bind(9, status.to_string().as_str()).map_err(|e| RepoError {
            target: "status".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let result = statement.execute().await.map_err(|e| RepoError {
            target: "job.jobs".to_string(),
            kind: ErrorKind::ExecuteError(e.into()),
        })?;

        if let Some(row) = result.first_row() {
            let app: bool = row.get_by_name("[applied]").map_err(|e| RepoError {
                target: "jobs - [applied] status".to_string(),
                kind: ExecuteError(e.into()),
            })?;

            if !app {
                return Err(RepoError {
                    target: name.0.clone(),
                    kind: RowAlreadyExists,
                });
            }
        }

        Ok(())
    }



    async fn get_job_info(&self, name: &JobName) -> Result<JobMetadata, RepoError> {
        let query = "SELECT name, backoff_duration, check_interval,  last_run, lock_ttl, max_retries, retry_attempts, schedule, state, status FROM jobs WHERE name = ?;";
        let mut statement = self.session.statement(query);

        statement.bind(0, name.0.as_str()).map_err(|e| RepoError {
            target: name.0.clone(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let result = statement.execute().await.map_err(|e| RepoError {
            target: "job.jobs".to_string(),
            kind: ErrorKind::ExecuteError(e.into()),
        })?;

        if let Some(row) = result.first_row() {
            let check_interval_secs: i64 = row
                .get_by_name("check_interval")
                .map_err(|_| RepoError {
                    target: "check_interval".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid check_interval".to_string()),
                })?;
            let lock_ttl_secs: i64 = row
                .get_by_name("lock_ttl")
                .map_err(|_| RepoError {
                    target: "lock_ttl".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid lock_ttl".to_string()),
                })?;
            let schedule_str: String = row
                .get_by_name("schedule")
                .map_err(|_| RepoError {
                    target: "schedule".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid schedule".to_string()),
                })?;
            let state_bytes: Vec<u8> = row
                .get_by_name("state")
                .map_err(|_| RepoError {
                    target: "state".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid state".to_string()),
                })?;
            let last_run_str: Option<String> = row.get_by_name("last_run").ok(); // `Option<String>` to handle NULL
            let retry_attempts: i32 = row
                .get_by_name("retry_attempts")
                .map_err(|_| RepoError {
                    target: "retry_attempts".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid retry_attempts".to_string()),
                })?;
            let max_retries: i32 = row
                .get_by_name("max_retries")
                .map_err(|_| RepoError {
                    target: "max_retries".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid max_retries".to_string()),
                })?;
            let backoff_duration_secs: i64 = row
                .get_by_name("backoff_duration")
                .map_err(|_| RepoError {
                    target: "backoff_duration".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid backoff_duration".to_string()),
                })?;

            let check_interval = Duration::from_secs(check_interval_secs as u64);
            let lock_ttl = Duration::from_secs(lock_ttl_secs as u64);
            let schedule = Schedule::from_str(&schedule_str).map_err(|_| RepoError {
                target: "schedule".to_string(),
                kind: ErrorKind::InvalidConfig("Schedule parse error".to_string()),
            })?;

            let state = Arc::new(Mutex::new(state_bytes));

            let last_run = match last_run_str {
                Some(ts) => DateTime::parse_from_rfc3339(&ts)
                    .map_err(|_e| RepoError {                        //TODO FIX ERROR
                        target: "last_run".to_string(),
                        kind: ErrorKind::InvalidConfig("Last run parse error".to_string()),
                    })?
                    .with_timezone(&Utc),
                None => Utc::now(),
            };

            let status: String = row
                .get_by_name("status")
                .map_err(|_| RepoError {
                    target: "status".to_string(),
                    kind: ErrorKind::InvalidConfig("Missing or invalid status".to_string()),
                })?;
            let job_status = JobStatus::from_string(&status);
            let backoff_duration = Duration::from_secs(backoff_duration_secs as u64);
            Ok(JobMetadata {
                name: name.clone(),
                backoff_duration,
                check_interval,
                last_run,
                lock_ttl,
                max_retries: max_retries as u32,
                retry_attempts: retry_attempts as u32,
                schedule,
                state,
                status: job_status
            })
        } else {
            Err(RepoError {
                target: name.0.clone(),
                kind: ErrorKind::NotFound,
            })
        }
    }


    async fn save_and_commit_state(&self, name: &JobName, status: JobStatus) -> Result<(), RepoError> {
        let mut statement = self.session.statement(
            "SELECT name FROM job.jobs WHERE name = ?;",
        );
        statement.bind(0, name.0.as_str()).map_err(|e| {
            RepoError {
                target: name.0.clone(),
                kind: ErrorKind::BindError(e.into()),
            }
        })?;

        let result = statement.execute().await.map_err(|e| {
            RepoError {
                target: "job.jobs".to_string(),
                kind: ErrorKind::ExecuteError(e.into()),
            }
        })?;

        if result.first_row().is_some() {
            eprintln!("Job found. Updating state for job: {:?}", name);
            let mut update_statement = self.session.statement(
                "UPDATE job.jobs SET status = ? WHERE name = ?;",
            );
            let status_str = status.to_string();
            update_statement.bind(0, status_str.as_str()).map_err(|e| {
                RepoError {
                    target: "status".to_string(),
                    kind: ErrorKind::BindError(e.into()),
                }
            })?;
            update_statement.bind(1, name.0.as_str()).map_err(|e| {
                RepoError {
                    target: name.0.clone(),
                    kind: ErrorKind::BindError(e.into()),
                }
            })?;

            update_statement.execute().await.map_err(|e| {
                RepoError {
                    target: "job.jobs".to_string(),
                    kind: ErrorKind::ExecuteError(e.into()),
                }
            })?;
        } else {
            eprintln!("Job not found. Cannot update state for non-existing job: {:?}", name);
            return Err(RepoError {
                target: name.0.clone(),
                kind: ErrorKind::NotFound,
            });
        }

        Ok(())
    }
    async fn acquire_lock(&self, job_name: &str) -> Result<bool, RepoError> {
        let exists_query = "SELECT job_name, lock_holder FROM job.locks WHERE job_name = ?";
        let mut check_statement = self.session.statement(exists_query);
        check_statement.bind(0, job_name).map_err(|e| RepoError {
            target: "job_name".to_string(),
            kind: ErrorKind::BindError(e.into()),
        })?;

        let exists_result = check_statement.execute().await.map_err(|e| RepoError {
            target: "job.locks".to_string(),
            kind: ErrorKind::ExecuteError(e.into()),
        })?;

        if let Some(row) = exists_result.first_row() {
            let lock_holder: Option<String> = match row.get_by_name::<String>("lock_holder".parse().unwrap()) {
               Ok(value) => Some(value),
                Err(_) => None,
            };

            if let Some(_holder) = lock_holder {            //TODO - Check this logic again
                return Ok(false);
            } else {
                let update_query = "UPDATE job.locks SET lock_holder = ?, lock_timestamp = toTimestamp(now()) WHERE job_name = ? IF lock_holder = NULL ";
                let mut update_statement = self.session.statement(update_query);
                update_statement.bind(0, self.hostname.as_str()).map_err(|e| RepoError {
                    target: "lock_holder".to_string(),
                    kind: ErrorKind::BindError(e.into()),
                })?;
                update_statement.bind(1, job_name).map_err(|e| RepoError {
                    target: "job_name".to_string(),
                    kind: ErrorKind::BindError(e.into()),
                })?;

                let update_result = update_statement.execute().await.map_err(|e| RepoError {
                    target: "job.locks".to_string(),
                    kind: ErrorKind::ExecuteError(e.into()),
                })?;

                if let Some(row) = update_result.first_row() {
                    let applied: bool = row.get_by_name("[applied]").map_err(|e| RepoError {
                        target: "job.locks - [applied] status".to_string(),
                        kind: ErrorKind::ColumnError(e.into()),
                    })?;
                    return Ok(applied);
                }
           }
        } else {
            let insert_query = "INSERT INTO job.locks (job_name, lock_holder, lock_timestamp) VALUES (?, ?, toTimestamp(now())) IF NOT EXISTS";
            let mut insert_statement = self.session.statement(insert_query);

            insert_statement.bind(0, job_name).map_err(|e| RepoError {
                target: "job_name".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;
            insert_statement.bind(1, self.hostname.as_str()).map_err(|e| RepoError {
                target: "lock_holder".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;

            let insert_result = insert_statement.execute().await.map_err(|e| RepoError {
                target: "job.locks".to_string(),
                kind: ErrorKind::ExecuteError(e.into()),
            })?;

            if let Some(row) = insert_result.first_row() {
                let applied: bool = row.get_by_name("[applied]").map_err(|e| RepoError {
                    target: "job.locks - [applied] status".to_string(),
                    kind: ErrorKind::ColumnError(e.into()),
                })?;
                return Ok(applied);
            }
        }

        Err(RepoError {
            target: job_name.to_string(),
            kind: ErrorKind::AcquireLockFailed("Failed to acquire lock due to unexpected error".to_string()),
        })
    }




    async fn release_lock(&self, job_name: &str) -> Result<(), RepoError> {
        let query = "UPDATE job.locks SET lock_holder = null WHERE job_name = ? IF lock_holder = ?";
        let mut statement = self.session.statement(query);
        statement.bind(0, job_name).map_err(|e| {
            RepoError {
                target: "state".to_string(),
                kind: ErrorKind::BindError(e.into()),
            }
        })?;
        statement
            .bind(1, self.hostname.as_str())
            .map_err(|e| RepoError {
                target: "lock_holder".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;

        let result =  statement.execute().await.map_err(|e| {
            RepoError {
                target: "job.jobs".to_string(),
                kind: ErrorKind::ExecuteError(e.into()),
            }
        })?;
        if let Some(row) = result.first_row() {
            let applied: bool = row.get_by_name("[applied]").map_err(|e| RepoError {
                target: "job.locks - [applied] status".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;

            return if applied {
                Ok(())
            } else {
                Err(RepoError {
                    target: job_name.to_string(),
                    kind: ErrorKind::AcquireLockFailed(
                        "Failed to release lock: lock_holder did not match.".to_string(),
                    ),
                })
            }
        }
        Err(RepoError {
            target: "job.locks".to_string(),
            kind: ErrorKind::AcquireLockFailed("Failed to release lock due to unknown reasons.".to_string()),
        })
    }

     async fn release_all_locks(&self) -> Result<(), RepoError> {
        let query = "UPDATE job.locks SET lock_holder = null WHERE job_name IN
                 (SELECT job_name FROM job.locks WHERE lock_holder = ?)
                 IF lock_holder = ?";

        let mut statement = self.session.statement(query);

        statement.bind(0, self.hostname.as_str()).map_err(|e| {
            RepoError {
                target: "lock_holder".to_string(),
                kind: ErrorKind::BindError(e.into()),
            }
        })?;

        statement.bind(1, self.hostname.as_str()).map_err(|e| {
            RepoError {
                target: "lock_holder".to_string(),
                kind: ErrorKind::BindError(e.into()),
            }
        })?;

        let result = statement.execute().await.map_err(|e| {
            RepoError {
                target: "job.locks".to_string(),
                kind: ErrorKind::ExecuteError(e.into()),
            }
        })?;

        if let Some(row) = result.first_row() {
            let applied: bool = row.get_by_name("[applied]").map_err(|e| RepoError {
                target: "job.locks - [applied] status".to_string(),
                kind: ErrorKind::BindError(e.into()),
            })?;

            return if applied {
                eprintln!("All locks released successfully.");
                Ok(())
            } else {
                Err(RepoError {
                    target: "job.locks".to_string(),
                    kind: ErrorKind::AcquireLockFailed(
                        "Failed to release all locks: lock_holder did not match.".to_string(),
                    ),
                })
            };
        }

        Err(RepoError {
            target: "job.locks".to_string(),
            kind: ErrorKind::AcquireLockFailed("Failed to release all locks due to unknown reasons.".to_string()),
        })
    }


    // async fn fetch_state(&self, id: String) -> Result<String, RepoError> {
    //     let query = "SELECT state FROM icp_device.state WHERE id = ?;";
    //     let mut statement = self.session.statement(query);
    //
    //     statement.bind_string(0, &id).map_err(|e| RepoError {
    //         target: "id".to_string(),
    //         kind: BindError(e.into()),
    //     })?;
    //
    //     let result = statement.execute().await.map_err(|e| RepoError {
    //         target: "icp_device.state".to_string(),
    //         kind: ExecuteError(e.into()),
    //     })?;
    //
    //     match result.first_row() {
    //         None => Err(RepoError {
    //             target: id.clone(),
    //             kind: ErrorKind::RowAlreadyExists,
    //         }),
    //         Some(row) => {
    //             let state: String = row.get_by_name("state").map_err(|e| RepoError {
    //                 target: "state".to_string(),
    //                 kind: ColumnError(e.into()),
    //             })?;
    //             Ok(state)
    //         }
    //     }
    // }

    // async fn get_job_state(&self, name: &JobName) -> Result<Arc<Mutex<Vec<u8>>>, RepoError> {
    //     let query = "SELECT state FROM jobs WHERE name = ?;";
    //     let mut statement = self.session.statement(query);
    //
    //     statement.bind(0, name.0.as_str()).map_err(|e| RepoError {
    //         target: name.0.clone(),
    //         kind: ErrorKind::BindError(e.into()),
    //     })?;
    //
    //     let result = statement.execute().await.map_err(|e| RepoError {
    //         target: "job.jobs".to_string(),
    //         kind: ErrorKind::ExecuteError(e.into()),
    //     })?;
    //
    //     if let Some(row) = result.first_row() {
    //         let state_bytes: Vec<u8> = row.get_by_name("state").unwrap();
    //         Ok(Arc::new(Mutex::new(state_bytes))) // Wrap it as needed
    //     } else {
    //         Err(RepoError {
    //             target: name.0.clone(),
    //             kind: ErrorKind::NotFound,
    //         })
    //     }
    // }
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
    AcquireLockFailed(String),
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
            ErrorKind::AcquireLockFailed(msg) => write!(
                f,
                "Lock acquire failed: {}. Reason: {}",
                self.target,msg
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
