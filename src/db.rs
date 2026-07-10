use crate::config::{DbConnectionConfig, GameDbOptions};
use crate::connection::{DbConnection, DbPool};
use crate::entity::DbEntity;
use crate::error::{Db233Error, Result};
use crate::named_params::{build_batch_named_params, replace_named_parameters};
use crate::orm::OrmHandler;
use crate::session::SessionRepository;
use crate::wal::LocalWriteJournal;
use mysql_async::Value;

pub struct Db {
    pool: DbPool,
    db_id: i32,
    db_type: DatabaseType,
    write_journal: Option<LocalWriteJournal>,
    session_repo: Option<std::sync::Arc<SessionRepository>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
}

impl Default for DatabaseType {
    fn default() -> Self {
        DatabaseType::MySQL
    }
}

impl Db {
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

    pub async fn query<T>(&self, sql: &str, params: &[Value]) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let mut conn = DbConnection::new(&self.pool).await?;
        let rows = conn.query(sql, params).await?;
        let results = OrmHandler::query_to_entity(rows).await?;
        Ok(results)
    }

    pub async fn query_map(&self, sql: &str, params: &[Value]) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let mut conn = DbConnection::new(&self.pool).await?;
        let rows = conn.query(sql, params).await?;
        OrmHandler::query_to_map(rows).await
    }

    pub async fn exec(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let mut conn = DbConnection::new(&self.pool).await?;
        conn.exec(sql, params).await
    }

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

    pub async fn update<T: DbEntity>(&self, entity: &T) -> Result<u64> {
        let (sql, values) = OrmHandler::build_update_sql(entity)?;
        let affected = self.exec(&sql, &values).await?;

        if let Some(wj) = &self.write_journal {
            wj.record_write(&sql, &values).await?;
        }

        Ok(affected)
    }

    pub async fn delete<T: DbEntity>(&self, entity: &T) -> Result<u64> {
        let (sql, values) = OrmHandler::build_delete_sql(entity);
        self.exec(&sql, &values).await
    }

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

    pub async fn query_named(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_map(&new_sql, &values).await
    }

    pub async fn query_named_to_int64(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<i64> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_int64(&new_sql, &values).await
    }

    pub async fn query_named_to_string(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<String> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_string(&new_sql, &values).await
    }

    pub async fn query_named_to_int64_slice(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<Vec<i64>> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_int64_slice(&new_sql, &values).await
    }

    pub async fn query_named_to_string_slice(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<Vec<String>> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.query_to_string_slice(&new_sql, &values).await
    }

    pub async fn exec_update_named(&self, sql: &str, params: &std::collections::HashMap<String, Value>) -> Result<u64> {
        let (new_sql, values) = replace_named_parameters(sql, params)?;
        self.exec(&new_sql, &values).await
    }

    pub async fn exec_update_multi_rows_named(&self, sql: &str, params_list: &[std::collections::HashMap<String, Value>]) -> Result<u64> {
        let (new_sql, batch_values) = build_batch_named_params(sql, params_list)?;
        let mut total_affected = 0;
        for values in batch_values {
            let affected = self.exec(&new_sql, &values).await?;
            total_affected += affected;
        }
        Ok(total_affected)
    }

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

    pub fn enable_wal(&mut self, path: &str) -> Result<()> {
        if self.write_journal.is_some() {
            return Ok(());
        }

        self.write_journal = Some(LocalWriteJournal::new(path)?);
        Ok(())
    }

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

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    pub fn db_id(&self) -> i32 {
        self.db_id
    }
}

impl Clone for Db {
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