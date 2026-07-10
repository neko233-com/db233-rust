use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub max_open_conns: usize,
    pub max_idle_conns: usize,
    pub conn_max_lifetime_sec: u64,
    pub conn_max_idle_time_sec: u64,
}

impl Default for DbConnectionConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "root".to_string(),
            database: "db233_rust".to_string(),
            max_open_conns: 50,
            max_idle_conns: 10,
            conn_max_lifetime_sec: 3600,
            conn_max_idle_time_sec: 600,
        }
    }
}

impl DbConnectionConfig {
    pub fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        database: &str,
    ) -> Self {
        Self {
            host: host.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            ..Default::default()
        }
    }

    pub fn to_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub concurrent_max_workers: usize,
    pub batch_upsert_chunk_size: usize,
    pub write_buffer_enabled: bool,
    pub write_buffer_flush_interval_ms: u64,
    pub max_open_conns: usize,
    pub max_idle_conns: usize,
    pub enable_local_journal: bool,
    pub local_journal_path: String,
    pub entity_cache: EntityCacheConfig,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            concurrent_max_workers: 16,
            batch_upsert_chunk_size: 200,
            write_buffer_enabled: true,
            write_buffer_flush_interval_ms: 100,
            max_open_conns: 100,
            max_idle_conns: 20,
            enable_local_journal: true,
            local_journal_path: "./data/db233_journal".to_string(),
            entity_cache: EntityCacheConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityCacheConfig {
    pub enabled: bool,
    pub eviction_policy: String,
    pub max_sessions: usize,
    pub session_flush_interval_ms: u64,
    pub flush_on_evict: bool,
    pub negative_cache_enabled: bool,
    pub entity_type_limits: std::collections::HashMap<String, usize>,
}

impl Default for EntityCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            eviction_policy: "lru".to_string(),
            max_sessions: 10000,
            session_flush_interval_ms: 60000,
            flush_on_evict: true,
            negative_cache_enabled: false,
            entity_type_limits: std::collections::HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameDbOptions {
    pub performance_config_path: Option<String>,
    pub enable_entity_cache: bool,
    pub cacheable_entities: Vec<CacheableEntitySpec>,
}

impl Default for GameDbOptions {
    fn default() -> Self {
        Self {
            performance_config_path: None,
            enable_entity_cache: true,
            cacheable_entities: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheableEntitySpec {
    pub prototype_name: String,
    pub max_instances: usize,
}

impl CacheableEntitySpec {
    pub fn new(prototype_name: &str, max_instances: usize) -> Self {
        Self {
            prototype_name: prototype_name.to_string(),
            max_instances,
        }
    }
}

pub fn load_performance_config(path: &str) -> Result<PerformanceConfig, crate::error::Db233Error> {
    let settings = config::Config::builder()
        .add_source(config::File::with_name(path))
        .build()
        .map_err(|e| crate::error::Db233Error::ConfigError(e.to_string()))?;

    settings
        .try_deserialize()
        .map_err(|e| crate::error::Db233Error::ConfigError(e.to_string()))
}