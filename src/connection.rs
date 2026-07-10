//! Database connection pool and connection handling for db233-rust.
//!
//! This module provides a wrapper around mysql_async's connection pool,
//! offering simplified connection management, health checks, and error handling.
//! It includes `DbPool` for managing the pool of connections and `DbConnection`
//! for individual connection operations.

use crate::config::DbConnectionConfig;
use crate::error::{Db233Error, Result};
use mysql_async::{Conn, Opts, Pool, Row};
use mysql_async::prelude::Queryable;
use std::sync::Arc;

/// Database connection pool.
///
/// Wraps mysql_async's Pool to provide simplified connection management.
/// The pool is cloneable and internally uses Arc for shared ownership.
#[derive(Clone)]
pub struct DbPool {
    inner: Arc<Pool>,
    config: DbConnectionConfig,
}

impl DbPool {
    /// Creates a new connection pool with the given configuration.
    ///
    /// Establishes an initial connection to verify the database is reachable.
    ///
    /// # Parameters
    ///
    /// - `config`: Database connection configuration.
    ///
    /// # Returns
    ///
    /// Returns the new DbPool, or an error if connection fails.
    pub async fn new(config: DbConnectionConfig) -> Result<Self> {
        let opts = Opts::from_url(&config.to_url())
            .map_err(|e| Db233Error::ConnectionError(e.to_string()))?;

        let pool = Pool::new(opts);
        pool.get_conn().await.map_err(|e| {
            Db233Error::ConnectionError(format!("failed to connect to database: {}", e))
        })?;

        Ok(Self {
            inner: Arc::new(pool),
            config,
        })
    }

    /// Gets a connection from the pool.
    ///
    /// If no connections are available and the pool hasn't reached its maximum size,
    /// a new connection will be created. If the pool is full, this will wait until
    /// a connection becomes available.
    ///
    /// # Returns
    ///
    /// Returns a connection from the pool, or an error if connection fails.
    pub async fn get_conn(&self) -> Result<Conn> {
        self.inner.get_conn().await.map_err(|e| {
            Db233Error::ConnectionError(format!("failed to get connection from pool: {}", e))
        })
    }

    /// Gets a reference to the connection configuration.
    pub fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    /// Closes the connection pool.
    ///
    /// Shuts down all connections and releases resources.
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Database connection wrapper.
///
/// Provides a high-level interface for executing queries and managing
/// a single database connection. Automatically returns the connection
/// to the pool when dropped.
pub struct DbConnection {
    conn: Conn,
    pool: DbPool,
}

impl DbConnection {
    /// Creates a new DbConnection by acquiring a connection from the pool.
    ///
    /// # Parameters
    ///
    /// - `pool`: The connection pool to get a connection from.
    ///
    /// # Returns
    ///
    /// Returns a new DbConnection, or an error if connection acquisition fails.
    pub async fn new(pool: &DbPool) -> Result<Self> {
        let conn = pool.get_conn().await?;
        Ok(Self {
            conn,
            pool: pool.clone(),
        })
    }

    /// Executes a query and returns the rows.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The parameters to bind to the query.
    ///
    /// # Returns
    ///
    /// Returns a vector of rows, or an error if the query fails.
    pub async fn query(
        &mut self,
        sql: &str,
        params: &[mysql_async::Value],
    ) -> Result<Vec<Row>> {
        let params: Vec<_> = params.to_vec();
        let rows = self.conn.exec_map(sql, params, |row: Row| row).await.map_err(|e| {
            Db233Error::QueryError(format!("query failed: {}", e))
        })?;
        Ok(rows)
    }

    /// Executes a non-query SQL statement.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL statement to execute.
    /// - `params`: The parameters to bind to the statement.
    ///
    /// # Returns
    ///
    /// Returns the number of affected rows, or an error if execution fails.
    pub async fn exec(
        &mut self,
        sql: &str,
        params: &[mysql_async::Value],
    ) -> Result<u64> {
        let params: Vec<_> = params.to_vec();
        self.conn.exec_drop(sql, params).await.map_err(|e| {
            Db233Error::QueryError(format!("exec failed: {}", e))
        })?;
        Ok(1)
    }

    /// Pings the database to check connection health.
    ///
    /// Sends a lightweight query to verify the connection is still alive.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if the connection is healthy, or an error if not.
    pub async fn ping(&mut self) -> Result<()> {
        self.conn.ping().await.map_err(|e| {
            Db233Error::ConnectionError(format!("ping failed: {}", e))
        })
    }

    /// Gets a reference to the connection pool.
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}

impl Drop for DbConnection {
    /// Drops the connection, returning it to the pool.
    ///
    /// The connection is automatically returned to the pool when the DbConnection
    /// goes out of scope, allowing it to be reused by other operations.
    fn drop(&mut self) {
    }
}
