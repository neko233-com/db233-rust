use crate::config::DbConnectionConfig;
use crate::error::{Db233Error, Result};
use mysql_async::{Conn, Opts, Pool, Row};
use mysql_async::prelude::Queryable;
use std::sync::Arc;

#[derive(Clone)]
pub struct DbPool {
    inner: Arc<Pool>,
    config: DbConnectionConfig,
}

impl DbPool {
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

    pub async fn get_conn(&self) -> Result<Conn> {
        self.inner.get_conn().await.map_err(|e| {
            Db233Error::ConnectionError(format!("failed to get connection from pool: {}", e))
        })
    }

    pub fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
}

pub struct DbConnection {
    conn: Conn,
    pool: DbPool,
}

impl DbConnection {
    pub async fn new(pool: &DbPool) -> Result<Self> {
        let conn = pool.get_conn().await?;
        Ok(Self {
            conn,
            pool: pool.clone(),
        })
    }

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

    pub async fn ping(&mut self) -> Result<()> {
        self.conn.ping().await.map_err(|e| {
            Db233Error::ConnectionError(format!("ping failed: {}", e))
        })
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}

impl Drop for DbConnection {
    fn drop(&mut self) {
    }
}