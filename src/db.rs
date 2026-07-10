//! Database operations and high-level API for db233-rust.
//!
//! This module provides the main `Db` struct which serves as the entry point for
//! all database operations. It wraps the connection pool and provides methods for:
//! - Entity CRUD operations (save, update, delete, find_by_id)
//! - Batch operations (save_batch_upsert)
//! - Named parameter queries
//! - WAL (Write-Ahead Log) integration
//! - Session repository initialization for game server scenarios

use crate::config::{DbConnectionConfig, GameDbOptions};
use crate::connection::{DbConnection, DbPool};
use crate::entity::DbEntity;
use crate::error::{Db233Error, Result};
use crate::named_params::{build_batch_named_params, replace_named_parameters};
use crate::orm::OrmHandler;
use crate::session::SessionRepository;
use crate::wal::LocalWriteJournal;
use mysql_async::Value;

/// Main database struct, serving as the entry point for all database operations.
///
/// Wraps the connection pool and provides high-level methods for querying,
/// inserting, updating, and deleting entities. Supports WAL for data durability
/// and session repository for game server scenarios.
#[derive(Clone)]
pub struct Db {
    /// Connection pool for database operations.
    pool: DbPool,
    /// Database instance ID (for multi-database scenarios).
    db_id: i32,
    /// Type of database (MySQL or PostgreSQL).
    db_type: DatabaseType,
    /// Optional Write-Ahead Log for data durability.
    write_journal: Option<LocalWriteJournal>,
    /// Optional session repository for game server entity caching.
    session_repo: Option<std::sync::Arc<SessionRepository>>,
}

/// Database type enumeration.
///
/// Currently supports MySQL and PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseType {
    /// MySQL database.
    MySQL,
    /// PostgreSQL database.
    PostgreSQL,
}

impl Default for DatabaseType {
    /// Returns MySQL as the default database type.
    fn default() -> Self {
        DatabaseType::MySQL
    }
}

impl Db {
    /// Creates a new database instance with the given connection configuration.
    ///
    /// # Parameters
    ///
    /// - `config`: Database connection configuration.
    /// - `db_id`: Database instance ID for multi-database scenarios.
    ///
    /// # Returns
    ///
    /// Returns the new Db instance, or an error if connection fails.
    pub async fn new(config: DbConnectionConfig, db_id: i32) -> Result<Self> {
        let pool = DbPool::new(config).await?;
        Ok(Self {
            pool,
            db_id,
            db_type: DatabaseType::MySQL,
            write_journal: None,
            session_repo: None,
        })
    }

    /// Executes a SQL query and returns the results as a vector of entities.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The positional parameters to bind to the query.
    ///
    /// # Returns
    ///
    /// Returns a vector of deserialized entities, or an error if the query fails.
    pub async fn query<T>(&self, sql: &str, params: &[Value]) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let mut conn = DbConnection::new(&self.pool).await?;
        let rows = conn.query(sql, params).await?;
        let results = OrmHandler::query_to_entity(rows).await?;
        Ok(results)
    }

    /// Executes a SQL query and returns the results as a vector of HashMaps.
    ///
    /// Each row is converted to a HashMap where keys are column names.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The positional parameters to bind to the query.
    ///
    /// # Returns
    ///
    /// Returns a vector of HashMaps, or an error if the query fails.
    pub async fn query_map(&self, sql: &str, params: &[Value]) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let mut conn = DbConnection::new(&self.pool).await?;
        let rows = conn.query(sql, params).await?;
        OrmHandler::query_to_map(rows).await
    }

    /// Executes a non-query SQL statement.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL statement to execute.
    /// - `params`: The positional parameters to bind to the statement.
    ///
    /// # Returns
    ///
    /// Returns the number of affected rows, or an error if execution fails.
    pub async fn exec(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let mut conn = DbConnection::new(&self.pool).await?;
        conn.exec(sql, params).await
    }

    /// Saves an entity using UPSERT semantics.
    ///
    /// If the entity's primary key is 0, it will be inserted. Otherwise, it will
    /// be updated if it exists. After insertion, the auto-generated ID is set on
    /// the entity.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to save (mutable to allow ID assignment).
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success, or an error if the operation fails.
    pub async fn save<T: DbEntity>(&self, entity: &mut T) -> Result<()> {
        let (sql, values) = OrmHandler::build_upsert_sql(entity)?;
        let affected = self.exec(&sql, &values).await?;

        if entity.primary_key_value() == 0 && affected > 0 {
            let last_insert_id = self.query_to_int64("SELECT LAST_INSERT_ID()", &[]).await?;
            entity.set_primary_key_value(last_insert_id);
        }

        if let Some(wj) = &self.write_journal {
            wj.record_write(&sql, &values).await?;
        }

        Ok(())
    }

    /// Updates an existing entity in the database.
    ///
    /// Uses the entity's primary key to locate and update the record.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to update.
    ///
    /// # Returns
    ///
    /// Returns the number of affected rows, or an error if the operation fails.
    pub async fn update<T: DbEntity>(&self, entity: &T) -> Result<u64> {
        let (sql, values) = OrmHandler::build_update_sql(entity)?;
        let affected = self.exec(&sql, &values).await?;

        if let Some(wj) = &self.write_journal {
            wj.record_write(&sql, &values).await?;
        }

        Ok(affected)
    }

    /// Deletes an entity from the database.
    ///
    /// Uses the entity's primary key to locate and delete the record.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to delete.
    ///
    /// # Returns
    ///
    /// Returns the number of affected rows, or an error if the operation fails.
    pub async fn delete<T: DbEntity>(&self, entity: &T) -> Result<u64> {
        let (sql, values) = OrmHandler::build_delete_sql(entity);
        self.exec(&sql, &values).await
    }

    /// Finds an entity by its primary key.
    ///
    /// # Parameters
    ///
    /// - `id`: The primary key value of the entity to find.
    ///
    /// # Returns
    ///
    /// Returns the found entity, or NotFound error if no entity matches.
    pub async fn find_by_id<T>(&self, id: i64) -> Result<T>
    where
        T: DbEntity + serde::de::DeserializeOwned + Default + Send + 'static,
    {
        let (sql, values) = OrmHandler::build_find_by_id_sql::<T>(id);
        let results = self.query::<T>(&sql, &values).await?;

        if results.is_empty() {
            return Err(Db233Error::NotFound);
        }

        Ok(results[0].clone())
    }

    /// Saves multiple entities using batch UPSERT.
    ///
    /// Uses MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` for efficient batch operations.
    ///
    /// # Parameters
    ///
    /// - `entities`: The entities to save in batch.
    ///
    /// # Returns
    ///
    /// Returns the number of affected rows, or an error if the operation fails.
    pub async fn save_batch_upsert<T: DbEntity>(&self, entities: &[T]) -> Result<u64> {
        if entities.is_empty() {
            return Ok(0);
        }

        let (sql, values) = OrmHandler::build_batch_upsert_sql(entities)?;
        let affected = self.exec(&sql, &values).await?;

        if let Some(wj) = &self.write_journal {
            wj.record_write(&sql, &values).await?;
        }

        Ok(affected)
    }

    /// Executes a SQL query with named parameters and returns results as HashMaps.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query with named parameters in `{param_name}` format.
    /// - `params`: A HashMap mapping parameter names to their values.
    ///
    /// # Returns
    ///
    /// Returns a vector of HashMaps, or an error if the query fails.
    pub async fn query_named(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_map(&new_sql, &values).await
    }

    /// Executes a SQL query with named parameters and returns a single i64 result.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query with named parameters.
    /// - `params`: A HashMap mapping parameter names to their values.
    ///
    /// # Returns
    ///
    /// Returns the first column of the first row as i64, or 0 if no results.
    pub async fn query_named_to_int64(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<i64> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_int64(&new_sql, &values).await
    }

    /// Executes a SQL query with named parameters and returns a single String result.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query with named parameters.
    /// - `params`: A HashMap mapping parameter names to their values.
    ///
    /// # Returns
    ///
    /// Returns the first column of the first row as String, or empty string if no results.
    pub async fn query_named_to_string(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<String> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_string(&new_sql, &values).await
    }

    /// Executes a SQL query with named parameters and returns a Vec<i64> result.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query with named parameters.
    /// - `params`: A HashMap mapping parameter names to their values.
    ///
    /// # Returns
    ///
    /// Returns the first column of each row as Vec<i64>.
    pub async fn query_named_to_int64_slice(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<Vec<i64>> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_int64_slice(&new_sql, &values).await
    }

    /// Executes a SQL query with named parameters and returns a Vec<String> result.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query with named parameters.
    /// - `params`: A HashMap mapping parameter names to their values.
    ///
    /// # Returns
    ///
    /// Returns the first column of each row as Vec<String>.
    pub async fn query_named_to_string_slice(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<Vec<String>> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_string_slice(&new_sql, &values).await
    }

    /// Executes an UPDATE statement with named parameters.
    ///
    /// # Parameters
    ///
    /// - `sql`: The UPDATE statement with named parameters.
    /// - `params`: A HashMap mapping parameter names to their values.
    ///
    /// # Returns
    ///
    /// Returns the number of affected rows, or an error if execution fails.
    pub async fn exec_update_named(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<u64> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.exec(&new_sql, &values).await
    }

    /// Executes the same UPDATE statement for multiple rows with different parameters.
    ///
    /// # Parameters
    ///
    /// - `sql`: The UPDATE statement with named parameters.
    /// - `params_list`: A slice of HashMaps, each representing parameters for one row.
    ///
    /// # Returns
    ///
    /// Returns the total number of affected rows across all executions.
    pub async fn exec_update_multi_rows_named(&self, sql: &str, params_list: &[std::collections::HashMap<String, Value>]) -> Result<u64> {
        let (new_sql, batch_values) = build_batch_named_params(sql, params_list)?;
        let mut total_affected = 0;
        for values in batch_values {
            let affected = self.exec(&new_sql, &values).await?;
            total_affected += affected;
        }
        Ok(total_affected)
    }

    /// Executes a SQL query and returns the first column of the first row as i64.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The positional parameters to bind.
    ///
    /// # Returns
    ///
    /// Returns the first column of the first row as i64, or 0 if no results.
    pub async fn query_to_int64(&self, sql: &str, params: &[Value]) -> Result<i64> {
        let results = self.query_map(sql, params).await?;
        if results.is_empty() {
            return Ok(0);
        }
        let row = &results[0];
        for (_, v) in row {
            if let mysql_async::Value::Int(i) = v {
                    return Ok(*i);
                } else if let mysql_async::Value::UInt(u) = v {
                    return Ok(*u as i64);
                }
        }
        Ok(0)
    }

    /// Executes a SQL query and returns the first column of the first row as String.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The positional parameters to bind.
    ///
    /// # Returns
    ///
    /// Returns the first column of the first row as String, or empty string if no results.
    pub async fn query_to_string(&self, sql: &str, params: &[Value]) -> Result<String> {
        let results = self.query_map(sql, params).await?;
        if results.is_empty() {
            return Ok(String::new());
        }
        let row = &results[0];
        for (_, v) in row {
            if let mysql_async::Value::Bytes(b) = v {
                if let Ok(s) = String::from_utf8(b.clone()) {
                    return Ok(s);
                }
            }
        }
        Ok(String::new())
    }

    /// Executes a SQL query and returns the first column of each row as Vec<i64>.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The positional parameters to bind.
    ///
    /// # Returns
    ///
    /// Returns the first column of each row as Vec<i64>.
    pub async fn query_to_int64_slice(&self, sql: &str, params: &[Value]) -> Result<Vec<i64>> {
        let results = self.query_map(sql, params).await?;
        let mut output = Vec::with_capacity(results.len());
        for row in results {
            for (_, v) in row {
                if let mysql_async::Value::Int(i) = v {
                    output.push(i);
                } else if let mysql_async::Value::UInt(u) = v {
                    output.push(u as i64);
                }
                break;
            }
        }
        Ok(output)
    }

    /// Executes a SQL query and returns the first column of each row as Vec<String>.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL query to execute.
    /// - `params`: The positional parameters to bind.
    ///
    /// # Returns
    ///
    /// Returns the first column of each row as Vec<String>.
    pub async fn query_to_string_slice(&self, sql: &str, params: &[Value]) -> Result<Vec<String>> {
        let results = self.query_map(sql, params).await?;
        let mut output = Vec::with_capacity(results.len());
        for row in results {
            for (_, v) in row {
                if let mysql_async::Value::Bytes(b) = v {
                    if let Ok(s) = String::from_utf8(b.clone()) {
                        output.push(s);
                    }
                }
                break;
            }
        }
        Ok(output)
    }

    /// Finds multiple entities by their primary keys.
    ///
    /// # Parameters
    ///
    /// - `ids`: The primary key values of the entities to find.
    ///
    /// # Returns
    ///
    /// Returns a vector of found entities.
    pub async fn find_by_ids<T>(&self, ids: &[i64]) -> Result<Vec<T>>
    where
        T: DbEntity + serde::de::DeserializeOwned + Send + 'static,
    {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = format!("{}", "?,".repeat(ids.len()).trim_end_matches(','));
        let sql = format!(
            "SELECT * FROM {} WHERE {} IN ({})",
            T::table_name(),
            T::primary_key_name(),
            placeholders
        );

        let values: Vec<Value> = ids.iter().map(|&id| Value::Int(id)).collect();
        self.query(&sql, &values).await
    }

    /// Finds multiple entities by their primary keys using concurrent queries.
    ///
    /// Splits the IDs into chunks and queries them concurrently using tokio tasks.
    /// This is faster than `find_by_ids` for large ID lists.
    ///
    /// # Parameters
    ///
    /// - `ids`: The primary key values of the entities to find.
    ///
    /// # Returns
    ///
    /// Returns a vector of found entities.
    pub async fn find_by_ids_concurrent<T>(&self, ids: &[i64]) -> Result<Vec<T>>
    where
        T: DbEntity + serde::de::DeserializeOwned + Send + 'static,
    {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let chunk_size = (ids.len() + 15) / 16;
        let chunks: Vec<&[i64]> = ids.chunks(chunk_size).collect();

        let mut tasks = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            let chunk = chunk.to_vec();
            let self_clone = self.clone();
            tasks.push(tokio::spawn(async move {
                self_clone.find_by_ids::<T>(&chunk).await
            }));
        }

        let mut results = Vec::new();
        for task in tasks {
            let chunk_results = task.await.map_err(|e| Db233Error::QueryError(format!("task join error: {}", e)));
            let chunk_results = chunk_results??;
            results.extend(chunk_results);
        }

        Ok(results)
    }

    /// Enables Write-Ahead Logging (WAL) for data durability.
    ///
    /// # Parameters
    ///
    /// - `path`: Path to the directory where WAL files will be stored.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success, or an error if WAL initialization fails.
    pub fn enable_wal(&mut self, path: &str) -> Result<()> {
        if self.write_journal.is_some() {
            return Ok(());
        }

        self.write_journal = Some(LocalWriteJournal::new(path)?);
        Ok(())
    }

    /// Initializes the database for game server scenarios.
    ///
    /// Sets up WAL, session repository, and entity caching based on the provided options.
    ///
    /// # Parameters
    ///
    /// - `_config`: Connection configuration (not used, kept for API compatibility).
    /// - `opts`: Game database options including performance config path and cache settings.
    ///
    /// # Returns
    ///
    /// Returns the session repository for managing player sessions and entity caching.
    pub async fn init_game_db(
        &mut self,
        _config: &DbConnectionConfig,
        opts: GameDbOptions,
    ) -> Result<std::sync::Arc<SessionRepository>> {
        let perf_config = opts
            .performance_config_path
            .as_ref()
            .map(|path| crate::config::load_performance_config(path))
            .transpose()?
            .unwrap_or_default();

        if perf_config.enable_local_journal {
            self.enable_wal(&perf_config.local_journal_path)?;
        }

        let session_repo = std::sync::Arc::new(SessionRepository::new(
            self.clone(),
            perf_config.entity_cache,
            opts.cacheable_entities,
        ).await?);

        self.session_repo = Some(session_repo.clone());
        Ok(session_repo)
    }

    /// Closes the database connection and cleans up resources.
    ///
    /// Flushes all sessions, stops the WAL, and closes the connection pool.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    pub async fn close(&self) -> Result<()> {
        if let Some(sr) = &self.session_repo {
            sr.flush_all().await?;
        }
        if let Some(wj) = &self.write_journal {
            wj.stop().await?;
        }
        self.pool.clone().close().await?;
        Ok(())
    }

    /// Gets a reference to the connection pool.
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Gets the database instance ID.
    pub fn db_id(&self) -> i32 {
        self.db_id
    }
}

impl Clone for Db {
    /// Clones the Db instance.
    ///
    /// The connection pool is shared via Arc. The write_journal is not cloned
    /// (set to None) to avoid duplicate WAL writing.
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            db_id: self.db_id,
            db_type: self.db_type.clone(),
            write_journal: None,
            session_repo: self.session_repo.clone(),
        }
    }
}