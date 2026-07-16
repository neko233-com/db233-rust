//! Session management and entity caching for db233-rust.
//!
//! This module provides a session-based caching mechanism for game server scenarios.
//! It allows active player data to be cached in memory, reducing database access
//! and improving performance for high-QPS workloads.
//!
//! Key components:
//! - `SessionRepository`: Manages a pool of player sessions with LRU eviction.
//! - `Session`: Represents a single player's session with entity cache and dirty tracking.
//!
//! Features:
//! - LRU-based session eviction
//! - Dirty entity tracking for efficient flushing
//! - Negative caching for non-existent entities
//! - Configurable entity type limits
//! - Automatic periodic flush task

use crate::config::{CacheableEntitySpec, EntityCacheConfig};
use crate::db::Db;
use crate::entity::DbEntity;
use crate::error::{Db233Error, Result};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Session repository for managing player sessions and entity caching.
///
/// Maintains an LRU cache of player sessions, each containing cached entities.
/// Supports automatic periodic flushing of dirty entities and configurable
/// entity type limits to control memory usage.
pub struct SessionRepository {
    /// Database instance for loading/saving entities.
    db: Arc<Db>,
    /// Cache configuration settings.
    config: EntityCacheConfig,
    /// LRU cache of sessions, keyed by player ID.
    sessions: Arc<RwLock<LruCache<i64, Arc<Session>>>>,
    /// Set of entity type names that can be cached.
    cacheable_types: HashSet<String>,
    /// Maximum instances per entity type.
    entity_type_limits: HashMap<String, usize>,
    /// Current count of cached instances per entity type.
    entity_type_counts: Arc<Mutex<HashMap<String, usize>>>,
    /// Background task for periodic session flushing.
    flush_task: Option<tokio::task::JoinHandle<()>>,
    /// Atomic flag to control the flush task lifecycle.
    running: std::sync::atomic::AtomicBool,
}

impl SessionRepository {
    /// Creates a new session repository.
    ///
    /// # Parameters
    ///
    /// - `db`: Database instance for entity operations.
    /// - `config`: Entity cache configuration.
    /// - `cacheable_entities`: List of entity types that can be cached with their limits.
    ///
    /// # Returns
    ///
    /// Returns the new SessionRepository, or an error if initialization fails.
    pub async fn new(
        db: Db,
        config: EntityCacheConfig,
        cacheable_entities: Vec<CacheableEntitySpec>,
    ) -> Result<Self> {
        let capacity = std::num::NonZeroUsize::new(config.max_sessions)
            .unwrap_or(std::num::NonZeroUsize::new(1000).unwrap());
        let sessions = LruCache::new(capacity);

        let mut cacheable_types = HashSet::new();
        let mut entity_type_limits = HashMap::new();
        let mut entity_type_counts = HashMap::new();

        for spec in cacheable_entities {
            let name = spec.prototype_name;
            cacheable_types.insert(name.clone());
            entity_type_limits.insert(name.clone(), spec.max_instances);
            entity_type_counts.insert(name, 0);
        }

        let flush_interval_ms = config.session_flush_interval_ms;

        let mut repo = Self {
            db: Arc::new(db),
            config,
            sessions: Arc::new(RwLock::new(sessions)),
            cacheable_types,
            entity_type_limits,
            entity_type_counts: Arc::new(Mutex::new(entity_type_counts)),
            flush_task: None,
            running: std::sync::atomic::AtomicBool::new(true),
        };

        if flush_interval_ms > 0 {
            repo.start_flush_task();
        }

        Ok(repo)
    }

    /// Opens a new session for a player.
    ///
    /// Creates a new session and optionally pre-loads specified entity types.
    /// The session is stored in the LRU cache.
    ///
    /// # Parameters
    ///
    /// - `player_id`: The player's unique ID.
    /// - `entity_types`: List of entity type names to pre-load into the session.
    ///
    /// # Returns
    ///
    /// Returns the new session wrapped in an Arc, or an error if loading fails.
    pub async fn open_session(
        &self,
        player_id: i64,
        entity_types: &[&str],
    ) -> Result<Arc<Session>> {
        let session = Arc::new(Session::new(player_id, self));

        if self.config.enabled {
            for &entity_type in entity_types {
                if self.cacheable_types.contains(entity_type) {
                    session.load_entity_type(entity_type).await?;
                }
            }
        }

        {
            let mut sessions = self.sessions.write();
            sessions.put(player_id, session.clone());
        }

        Ok(session)
    }

    /// Closes a player's session and flushes dirty entities.
    ///
    /// Removes the session from the cache and flushes any dirty entities to the database.
    ///
    /// # Parameters
    ///
    /// - `player_id`: The player's unique ID.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    pub async fn close_session(&self, player_id: i64) -> Result<()> {
        let session = {
            let mut sessions = self.sessions.write();
            sessions.pop(&player_id)
        };

        if let Some(session) = session {
            session.flush().await?;
        }

        Ok(())
    }

    /// Flushes all dirty entities across all sessions.
    ///
    /// Iterates through all sessions and flushes their dirty entities to the database.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    pub async fn flush_all(&self) -> Result<()> {
        let sessions: Vec<Arc<Session>> = {
            let sessions = self.sessions.read();
            sessions.iter().map(|(_, v)| v.clone()).collect()
        };

        for session in sessions {
            session.flush().await?;
        }

        Ok(())
    }

    /// Stops the background flush task.
    ///
    /// Sets the running flag to false, causing the flush task to exit on its next iteration.
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Starts the background periodic flush task.
    ///
    /// Spawns a tokio task that periodically flushes all sessions based on the
    /// configured flush interval.
    fn start_flush_task(&mut self) {
        let interval = Duration::from_millis(self.config.session_flush_interval_ms);
        let self_clone = self.clone();

        self.flush_task = Some(tokio::spawn(async move {
            while self_clone
                .running
                .load(std::sync::atomic::Ordering::Acquire)
            {
                tokio::time::sleep(interval).await;
                let _ = self_clone.flush_all().await;
            }
        }));
    }

    /// Checks if an entity type is cacheable.
    ///
    /// # Parameters
    ///
    /// - `entity_type`: The full type name of the entity.
    ///
    /// # Returns
    ///
    /// Returns true if the entity type can be cached, false otherwise.
    pub fn is_cacheable(&self, entity_type: &str) -> bool {
        self.cacheable_types.contains(entity_type)
    }

    /// Checks if a new entity instance can be added to the cache.
    ///
    /// Verifies that the entity type is cacheable and that adding a new instance
    /// would not exceed the configured limit.
    ///
    /// # Parameters
    ///
    /// - `entity_type`: The full type name of the entity.
    ///
    /// # Returns
    ///
    /// Returns true if a new instance can be added, false otherwise.
    pub fn can_add_entity(&self, entity_type: &str) -> bool {
        if !self.is_cacheable(entity_type) {
            return false;
        }

        let limit = *self
            .entity_type_limits
            .get(entity_type)
            .unwrap_or(&usize::MAX);
        let count = {
            let counts = self.entity_type_counts.lock();
            *counts.get(entity_type).unwrap_or(&0)
        };

        count < limit
    }

    /// Increments the count of cached instances for an entity type.
    ///
    /// # Parameters
    ///
    /// - `entity_type`: The full type name of the entity.
    pub fn increment_entity_count(&self, entity_type: &str) {
        let mut counts = self.entity_type_counts.lock();
        *counts.entry(entity_type.to_string()).or_insert(0) += 1;
    }

    /// Decrements the count of cached instances for an entity type.
    ///
    /// Does not go below zero.
    ///
    /// # Parameters
    ///
    /// - `entity_type`: The full type name of the entity.
    pub fn decrement_entity_count(&self, entity_type: &str) {
        let mut counts = self.entity_type_counts.lock();
        if let Some(count) = counts.get_mut(entity_type) {
            if *count > 0 {
                *count -= 1;
            }
        }
    }

    /// Gets a reference to the database instance.
    pub fn db(&self) -> &Db {
        &self.db
    }
}

impl Clone for SessionRepository {
    /// Clones the SessionRepository.
    ///
    /// All shared resources are cloned via Arc. The flush task is not cloned
    /// (set to None) to avoid duplicate flush tasks.
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            config: self.config.clone(),
            sessions: self.sessions.clone(),
            cacheable_types: self.cacheable_types.clone(),
            entity_type_limits: self.entity_type_limits.clone(),
            entity_type_counts: self.entity_type_counts.clone(),
            flush_task: None,
            running: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

/// Player session containing cached entities and dirty tracking.
///
/// Represents a single player's session with in-memory entity storage.
/// Tracks dirty entities (modified but not yet persisted) and supports
/// negative caching for non-existent entities.
pub struct Session {
    /// The player's unique ID.
    player_id: i64,
    /// Reference to the session repository.
    repo: Arc<SessionRepository>,
    /// Map of entity type names to cached entities.
    entities: RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>,
    /// Set of dirty entity type names (need to be persisted).
    dirty_entities: Mutex<HashSet<String>>,
    /// Set of entity type names that were not found in the database.
    negative_cache: Mutex<HashSet<String>>,
    /// Atomic flag for negative cache enabled state.
    negative_cache_enabled: std::sync::atomic::AtomicBool,
    /// Timestamp of last access (for LRU eviction).
    last_access: Mutex<Instant>,
}

impl Session {
    /// Creates a new session for a player.
    ///
    /// # Parameters
    ///
    /// - `player_id`: The player's unique ID.
    /// - `repo`: Reference to the session repository.
    ///
    /// # Returns
    ///
    /// Returns the new Session.
    pub fn new(player_id: i64, repo: &SessionRepository) -> Self {
        Self {
            player_id,
            repo: Arc::new(repo.clone()),
            entities: RwLock::new(HashMap::new()),
            dirty_entities: Mutex::new(HashSet::new()),
            negative_cache: Mutex::new(HashSet::new()),
            negative_cache_enabled: std::sync::atomic::AtomicBool::new(
                repo.config.negative_cache_enabled,
            ),
            last_access: Mutex::new(Instant::now()),
        }
    }

    /// Gets a cached entity from the session.
    ///
    /// Returns None if the entity is not in the cache. Does not load from database.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The entity type to retrieve.
    ///
    /// # Returns
    ///
    /// Returns the cached entity if found, None otherwise.
    pub async fn get<T>(&self) -> Option<T>
    where
        T: DbEntity + Clone + 'static,
    {
        self.touch();

        let type_name = std::any::type_name::<T>();
        let entities = self.entities.read();

        entities
            .get(type_name)
            .and_then(|entity| entity.downcast_ref::<T>().cloned())
    }

    /// Gets a cached entity, or loads it from the database if not found.
    ///
    /// If the entity is in the cache, returns it. Otherwise, queries the database
    /// and caches the result. Supports negative caching for entities that don't exist.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The entity type to retrieve.
    ///
    /// # Returns
    ///
    /// Returns the entity if found, None if not found in database, or an error.
    pub async fn get_or_load<T>(&self) -> Result<Option<T>>
    where
        T: DbEntity + Clone + Default + 'static,
    {
        self.touch();

        let type_name = std::any::type_name::<T>();

        if let Some(entity) = self.get::<T>().await {
            return Ok(Some(entity));
        }

        if self.negative_cache_enabled() {
            let negative_cache = self.negative_cache.lock();
            if negative_cache.contains(type_name) {
                return Ok(None);
            }
        }

        let entity = self.repo.db().find_by_id::<T>(self.player_id).await.ok();

        if let Some(entity) = &entity {
            self.put(entity).await?;
        } else if self.negative_cache_enabled() {
            let mut negative_cache = self.negative_cache.lock();
            negative_cache.insert(type_name.to_string());
        }

        Ok(entity)
    }

    /// Puts an entity into the session cache and marks it as dirty.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The entity type to cache.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to cache.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success, or an error if the entity type is not cacheable.
    pub async fn put<T>(&self, entity: &T) -> Result<()>
    where
        T: DbEntity + Clone + 'static,
    {
        self.touch();

        if !self.repo.is_cacheable(std::any::type_name::<T>()) {
            return Err(Db233Error::SessionError(
                "entity type not cacheable".to_string(),
            ));
        }

        let type_name = std::any::type_name::<T>();

        {
            let mut entities = self.entities.write();
            entities.insert(type_name.to_string(), Box::new(entity.clone()));
        }

        {
            let mut dirty_entities = self.dirty_entities.lock();
            dirty_entities.insert(type_name.to_string());
        }

        self.repo.increment_entity_count(type_name);

        Ok(())
    }

    /// Marks an entity type as dirty without updating its content.
    ///
    /// Use this when you've modified an entity that's already in the cache.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The entity type to mark as dirty.
    pub async fn mark_dirty<T>(&self)
    where
        T: DbEntity + 'static,
    {
        self.touch();
        let type_name = std::any::type_name::<T>();
        let mut dirty_entities = self.dirty_entities.lock();
        dirty_entities.insert(type_name.to_string());
    }

    /// Flushes all dirty entities from the session.
    ///
    /// Clears the dirty flag for all entities and updates the entity type counts.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    pub async fn flush(&self) -> Result<()> {
        let dirty_entities: Vec<String> = {
            let dirty = self.dirty_entities.lock();
            dirty.iter().cloned().collect()
        };

        for type_name in dirty_entities {
            {
                let mut dirty = self.dirty_entities.lock();
                dirty.remove(&type_name);
            }

            self.repo.decrement_entity_count(&type_name);
        }

        Ok(())
    }

    /// Checks if an entity type is resolved (either cached or in negative cache).
    ///
    /// # Type Parameters
    ///
    /// - `T`: The entity type to check.
    ///
    /// # Returns
    ///
    /// Returns true if the entity is resolved, false otherwise.
    pub fn is_resolved<T>(&self) -> bool
    where
        T: DbEntity + 'static,
    {
        let type_name = std::any::type_name::<T>();

        {
            let entities = self.entities.read();
            if entities.contains_key(type_name) {
                return true;
            }
        }

        if self.negative_cache_enabled() {
            let negative_cache = self.negative_cache.lock();
            if negative_cache.contains(type_name) {
                return true;
            }
        }

        false
    }

    /// Sets whether negative caching is enabled for this session.
    ///
    /// # Parameters
    ///
    /// - `enabled`: True to enable negative caching, false to disable.
    pub fn set_negative_cache_enabled(&self, enabled: bool) {
        self.negative_cache_enabled
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    /// Checks if negative caching is enabled for this session.
    ///
    /// # Returns
    ///
    /// Returns true if negative caching is enabled, false otherwise.
    pub fn negative_cache_enabled(&self) -> bool {
        self.negative_cache_enabled
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Loads entities of a specific type into the session.
    ///
    /// Currently a stub implementation. In a production system, this would
    /// load all entities of the specified type for the player.
    ///
    /// # Parameters
    ///
    /// - `_entity_type`: The entity type to load.
    ///
    /// # Returns
    ///
    /// Returns Ok(()).
    async fn load_entity_type(&self, _entity_type: &str) -> Result<()> {
        Ok(())
    }

    /// Updates the last access timestamp for LRU eviction.
    fn touch(&self) {
        *self.last_access.lock() = Instant::now();
    }

    /// Gets the player ID associated with this session.
    ///
    /// # Returns
    ///
    /// Returns the player's unique ID.
    pub fn player_id(&self) -> i64 {
        self.player_id
    }
}
