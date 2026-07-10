use thiserror::Error;

#[derive(Error, Debug)]
pub enum Db233Error {
    #[error("database connection error: {0}")]
    ConnectionError(String),

    #[error("query error: {0}")]
    QueryError(String),

    #[error("parameter error: {0}")]
    ParameterError(String),

    #[error("entity mapping error: {0}")]
    MappingError(String),

    #[error("session error: {0}")]
    SessionError(String),

    #[error("WAL error: {0}")]
    WalError(String),

    #[error("config error: {0}")]
    ConfigError(String),

    #[error("timeout")]
    Timeout,

    #[error("not found")]
    NotFound,

    #[error("unknown error: {0}")]
    Unknown(String),
}

impl From<mysql_async::Error> for Db233Error {
    fn from(e: mysql_async::Error) -> Self {
        match e {
            mysql_async::Error::Io(_) => {
                Db233Error::ConnectionError(e.to_string())
            }
            _ => Db233Error::QueryError(e.to_string()),
        }
    }
}

impl From<std::io::Error> for Db233Error {
    fn from(e: std::io::Error) -> Self {
        Db233Error::WalError(e.to_string())
    }
}

impl From<serde_json::Error> for Db233Error {
    fn from(e: serde_json::Error) -> Self {
        Db233Error::MappingError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Db233Error>;