//! Error types and result type for db233-rust.
//!
//! This module defines the custom error type `Db233Error` that covers all possible
//! error scenarios in the database library, including connection errors, query errors,
//! parameter errors, mapping errors, session errors, WAL errors, and configuration errors.
//!
//! It also provides `From` implementations to convert common external errors
//! (mysql_async, std::io, serde_json) into `Db233Error`.

use thiserror::Error;

/// The main error type for db233-rust.
///
/// Covers all possible error scenarios in the database library.
#[derive(Error, Debug)]
pub enum Db233Error {
    /// Database connection error (e.g., network issues, authentication failure).
    #[error("database connection error: {0}")]
    ConnectionError(String),

    /// Query execution error (e.g., SQL syntax error, constraint violation).
    #[error("query error: {0}")]
    QueryError(String),

    /// Parameter error (e.g., missing required parameter, invalid parameter type).
    #[error("parameter error: {0}")]
    ParameterError(String),

    /// Entity mapping error (e.g., failed to serialize/deserialize entity).
    #[error("entity mapping error: {0}")]
    MappingError(String),

    /// Session error (e.g., session not found, cache eviction issues).
    #[error("session error: {0}")]
    SessionError(String),

    /// Write-Ahead Log error (e.g., failed to write to WAL file).
    #[error("WAL error: {0}")]
    WalError(String),

    /// Configuration error (e.g., invalid config file, missing config value).
    #[error("config error: {0}")]
    ConfigError(String),

    /// Operation timed out.
    #[error("timeout")]
    Timeout,

    /// Entity or record not found.
    #[error("not found")]
    NotFound,

    /// Unknown error (catch-all for unexpected errors).
    #[error("unknown error: {0}")]
    Unknown(String),
}

/// Convert mysql_async::Error to Db233Error.
///
/// IO errors are converted to ConnectionError, all others to QueryError.
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

/// Convert std::io::Error to Db233Error.
///
/// IO errors are typically related to WAL file operations.
impl From<std::io::Error> for Db233Error {
    fn from(e: std::io::Error) -> Self {
        Db233Error::WalError(e.to_string())
    }
}

/// Convert serde_json::Error to Db233Error.
///
/// JSON errors are related to entity serialization/deserialization.
impl From<serde_json::Error> for Db233Error {
    fn from(e: serde_json::Error) -> Self {
        Db233Error::MappingError(e.to_string())
    }
}

/// Result type alias for db233-rust operations.
///
/// All database operations return this type, which is a Result with Db233Error as the error type.
pub type Result<T> = std::result::Result<T, Db233Error>;
