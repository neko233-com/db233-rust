pub mod config;
pub mod connection;
pub mod db;
pub mod entity;
pub mod error;
pub mod named_params;
pub mod orm;
pub mod session;
pub mod wal;

#[cfg(test)]
pub mod orm_tests;

#[cfg(test)]
pub mod entity_tests;

pub use config::*;
pub use connection::*;
pub use db::*;
pub use entity::*;
pub use error::*;
pub use named_params::*;
pub use orm::*;
pub use session::*;
pub use wal::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[tokio::test]
    async fn test_db_connection_config_default() {
        let config = DbConnectionConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 3306);
        assert_eq!(config.max_open_conns, 50);
        assert_eq!(config.max_idle_conns, 10);
    }

    #[tokio::test]
    async fn test_db_connection_config_new() {
        let config = DbConnectionConfig::new("localhost", 3307, "user", "pass", "test_db");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3307);
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
        assert_eq!(config.database, "test_db");
    }

    #[tokio::test]
    async fn test_db_connection_config_to_url() {
        let config = DbConnectionConfig::new("localhost", 3306, "user", "pass", "test");
        let url = config.to_url();
        assert_eq!(url, "mysql://user:pass@localhost:3306/test");
    }

    #[tokio::test]
    async fn test_performance_config_default() {
        let config = PerformanceConfig::default();
        assert_eq!(config.concurrent_max_workers, 16);
        assert_eq!(config.batch_upsert_chunk_size, 200);
        assert!(config.write_buffer_enabled);
    }

    #[tokio::test]
    async fn test_entity_cache_config_default() {
        let config = EntityCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.eviction_policy, "lru");
        assert_eq!(config.max_sessions, 10000);
    }

    #[tokio::test]
    async fn test_game_db_options_default() {
        let opts = GameDbOptions::default();
        assert!(opts.performance_config_path.is_none());
        assert!(opts.enable_entity_cache);
        assert!(opts.cacheable_entities.is_empty());
    }

    #[tokio::test]
    async fn test_cacheable_entity_spec() {
        let spec = CacheableEntitySpec::new("Player", 8000);
        assert_eq!(spec.prototype_name, "Player");
        assert_eq!(spec.max_instances, 8000);
    }
}
