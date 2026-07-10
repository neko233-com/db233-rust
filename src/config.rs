//! Configuration structures and loading for db233-rust.
//!
//! This module defines all configuration structures used by the database library,
//! including connection configuration, performance configuration, entity cache configuration,
//! and game database options. It also provides a function to load performance configuration
//! from JSON files.

use serde::{Deserialize, Serialize};

/// Database connection configuration.
///
/// Contains all parameters needed to establish a connection to a MySQL database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConnectionConfig {
    /// Database host address (IP or hostname).
    pub host: String,

    /// Database port (default: 3306 for MySQL).
    pub port: u16,

    /// Database username for authentication.
    pub username: String,

    /// Database password for authentication.
    pub password: String,

    /// Database name to connect to.
    pub database: String,

    /// Maximum number of open connections in the pool.
    pub max_open_conns: usize,

    /// Maximum number of idle connections in the pool.
    pub max_idle_conns: usize,

    /// Maximum lifetime of a connection in seconds.
    pub conn_max_lifetime_sec: u64,

    /// Maximum idle time for a connection in seconds.
    pub conn_max_idle_time_sec: u64,
}

impl Default for DbConnectionConfig {
    /// Creates a default connection configuration.
    ///
    /// Default values:
    /// - host: "127.0.0.1"
    /// - port: 3306
    /// - username: "root"
    /// - password: "root"
    /// - database: "db233_rust"
    /// - max_open_conns: 50
    /// - max_idle_conns: 10
    /// - conn_max_lifetime_sec: 3600
    /// - conn_max_idle_time_sec: 600
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
    /// Creates a new connection configuration with the required parameters.
    ///
    /// Uses default values for connection pool settings.
    ///
    /// # Parameters
    ///
    /// - `host`: Database host address.
    /// - `port`: Database port.
    /// - `username`: Database username.
    /// - `password`: Database password.
    /// - `database`: Database name.
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

    /// Converts the configuration to a MySQL connection URL.
    ///
    /// Format: `mysql://username:password@host:port/database`
    pub fn to_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

/// Performance configuration for the database library.
///
/// Contains settings that control performance-related behavior, such as connection pool
/// size, batch processing, write buffering, and WAL settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum number of concurrent workers for parallel operations.
    pub concurrent_max_workers: usize,

    /// Chunk size for batch upsert operations.
    pub batch_upsert_chunk_size: usize,

    /// Whether write buffering is enabled.
    pub write_buffer_enabled: bool,

    /// Interval (in milliseconds) for flushing the write buffer.
    pub write_buffer_flush_interval_ms: u64,

    /// Maximum number of open connections (overrides connection config).
    pub max_open_conns: usize,

    /// Maximum number of idle connections (overrides connection config).
    pub max_idle_conns: usize,

    /// Whether local WAL (Write-Ahead Log) is enabled.
    pub enable_local_journal: bool,

    /// Path to the local WAL journal files.
    pub local_journal_path: String,

    /// Configuration for entity caching.
    pub entity_cache: EntityCacheConfig,
}

impl Default for PerformanceConfig {
    /// Creates default performance configuration.
    ///
    /// Default values:
    /// - concurrent_max_workers: 16
    /// - batch_upsert_chunk_size: 200
    /// - write_buffer_enabled: true
    /// - write_buffer_flush_interval_ms: 100
    /// - max_open_conns: 100
    /// - max_idle_conns: 20
    /// - enable_local_journal: true
    /// - local_journal_path: "./data/db233_journal"
    /// - entity_cache: default EntityCacheConfig
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

/// Entity cache configuration.
///
/// Controls the behavior of the session-level entity cache, including eviction policy,
/// cache size, and flushing behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityCacheConfig {
    /// Whether entity caching is enabled.
    pub enabled: bool,

    /// Eviction policy for the cache ("lru" or other policies).
    pub eviction_policy: String,

    /// Maximum number of sessions in the cache.
    pub max_sessions: usize,

    /// Interval (in milliseconds) for automatic session flushing.
    pub session_flush_interval_ms: u64,

    /// Whether to flush dirty entities when evicting a session.
    pub flush_on_evict: bool,

    /// Whether negative caching is enabled (cache missing entities).
    pub negative_cache_enabled: bool,

    /// Per-entity-type cache limits (entity type name -> max instances).
    pub entity_type_limits: std::collections::HashMap<String, usize>,
}

impl Default for EntityCacheConfig {
    /// Creates default entity cache configuration.
    ///
    /// Default values:
    /// - enabled: true
    /// - eviction_policy: "lru"
    /// - max_sessions: 10000
    /// - session_flush_interval_ms: 60000 (1 minute)
    /// - flush_on_evict: true
    /// - negative_cache_enabled: false
    /// - entity_type_limits: empty
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

/// Options for initializing a game database.
///
/// Contains settings specific to game server scenarios, including performance
/// configuration and cacheable entity specifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameDbOptions {
    /// Optional path to a performance configuration JSON file.
    pub performance_config_path: Option<String>,

    /// Whether entity caching is enabled.
    pub enable_entity_cache: bool,

    /// List of entity types that can be cached, with their instance limits.
    pub cacheable_entities: Vec<CacheableEntitySpec>,
}

impl Default for GameDbOptions {
    /// Creates default game database options.
    ///
    /// Default values:
    /// - performance_config_path: None
    /// - enable_entity_cache: true
    /// - cacheable_entities: empty
    fn default() -> Self {
        Self {
            performance_config_path: None,
            enable_entity_cache: true,
            cacheable_entities: Vec::new(),
        }
    }
}

/// Specification for a cacheable entity type.
///
/// Defines which entity types can be cached and their maximum instance count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheableEntitySpec {
    /// Full type name of the entity (e.g., "db233::examples::PlayerBaseEntity").
    pub prototype_name: String,

    /// Maximum number of instances to cache for this entity type.
    pub max_instances: usize,
}

impl CacheableEntitySpec {
    /// Creates a new cacheable entity specification.
    ///
    /// # Parameters
    ///
    /// - `prototype_name`: Full type name of the entity.
    /// - `max_instances`: Maximum number of instances to cache.
    pub fn new(prototype_name: &str, max_instances: usize) -> Self {
        Self {
            prototype_name: prototype_name.to_string(),
            max_instances,
        }
    }
}

/// Loads performance configuration from a JSON file.
///
/// # Parameters
///
/// - `path`: Path to the JSON configuration file.
///
/// # Returns
///
/// Returns the parsed PerformanceConfig, or an error if loading fails.
pub fn load_performance_config(path: &str) -> Result<PerformanceConfig, crate::error::Db233Error> {
    let settings = config::Config::builder()
        .add_source(config::File::with_name(path))
        .build()
        .map_err(|e| crate::error::Db233Error::ConfigError(e.to_string()))?;

    settings
        .try_deserialize()
        .map_err(|e| crate::error::Db233Error::ConfigError(e.to_string()))
}
