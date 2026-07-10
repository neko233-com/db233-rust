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

pub struct SessionRepository {
    db: Arc<Db>,
    config: EntityCacheConfig,
    sessions: Arc<RwLock<LruCache<i64, Arc<Session>>>>,
    cacheable_types: HashSet<String>,
    entity_type_limits: HashMap<String, usize>,
    entity_type_counts: Arc<Mutex<HashMap<String, usize>>>,
    flush_task: Option<tokio::task::JoinHandle<()>>,
    running: std::sync::atomic::AtomicBool,
}

impl SessionRepository {
    pub async fn new(
        db: Db,
        config: EntityCacheConfig,
        cacheable_entities: Vec<CacheableEntitySpec>,
    ) -> Result<Self> {
        let capacity = std::num::NonZeroUsize::new(config.max_sessions).unwrap_or(std::num::NonZeroUsize::new(1000).unwrap());
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

    pub async fn open_session(&self, player_id: i64, entity_types: &[&str]) -> Result<Arc<Session>> {
        let session = Arc::new(Session::new(player_id, self));

        if self.config.enabled {
            for entity_type in entity_types {
                if self.cacheable_types.contains(*entity_type) {
                    session.load_entity_type(*entity_type).await?;
                }
            }
        }

        {
            let mut sessions = self.sessions.write();
            sessions.put(player_id, session.clone());
        }

        Ok(session)
    }

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

    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Release);
    }

    fn start_flush_task(&mut self) {
        let interval = Duration::from_millis(self.config.session_flush_interval_ms);
        let self_clone = self.clone();

        self.flush_task = Some(tokio::spawn(async move {
            while self_clone.running.load(std::sync::atomic::Ordering::Acquire) {
                tokio::time::sleep(interval).await;
                let _ = self_clone.flush_all().await;
            }
        }));
    }

    pub fn is_cacheable(&self, entity_type: &str) -> bool {
        self.cacheable_types.contains(entity_type)
    }

    pub fn can_add_entity(&self, entity_type: &str) -> bool {
        if !self.is_cacheable(entity_type) {
            return false;
        }

        let limit = *self.entity_type_limits.get(entity_type).unwrap_or(&usize::MAX);
        let count = {
            let counts = self.entity_type_counts.lock();
            *counts.get(entity_type).unwrap_or(&0)
        };

        count < limit
    }

    pub fn increment_entity_count(&self, entity_type: &str) {
        let mut counts = self.entity_type_counts.lock();
        *counts.entry(entity_type.to_string()).or_insert(0) += 1;
    }

    pub fn decrement_entity_count(&self, entity_type: &str) {
        let mut counts = self.entity_type_counts.lock();
        if let Some(count) = counts.get_mut(entity_type) {
            if *count > 0 {
                *count -= 1;
            }
        }
    }

    pub fn db(&self) -> &Db {
        &self.db
    }
}

impl Clone for SessionRepository {
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

pub struct Session {
    player_id: i64,
    repo: Arc<SessionRepository>,
    entities: RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>,
    dirty_entities: Mutex<HashSet<String>>,
    negative_cache: Mutex<HashSet<String>>,
    negative_cache_enabled: std::sync::atomic::AtomicBool,
    last_access: Mutex<Instant>,
}

impl Session {
    pub fn new(player_id: i64, repo: &SessionRepository) -> Self {
        Self {
            player_id,
            repo: Arc::new(repo.clone()),
            entities: RwLock::new(HashMap::new()),
            dirty_entities: Mutex::new(HashSet::new()),
            negative_cache: Mutex::new(HashSet::new()),
            negative_cache_enabled: std::sync::atomic::AtomicBool::new(repo.config.negative_cache_enabled),
            last_access: Mutex::new(Instant::now()),
        }
    }

    pub async fn get<T>(&self) -> Option<T>
    where
        T: DbEntity + Clone + 'static,
    {
        self.touch();

        let type_name = std::any::type_name::<T>();
        let entities = self.entities.read();

        entities.get(type_name).and_then(|entity| {
            entity.downcast_ref::<T>().cloned()
        })
    }

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

    pub async fn put<T>(&self, entity: &T) -> Result<()>
    where
        T: DbEntity + Clone + 'static,
    {
        self.touch();

        if !self.repo.is_cacheable(std::any::type_name::<T>()) {
            return Err(Db233Error::SessionError("entity type not cacheable".to_string()));
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

    pub async fn mark_dirty<T>(&self)
    where
        T: DbEntity + 'static,
    {
        self.touch();
        let type_name = std::any::type_name::<T>();
        let mut dirty_entities = self.dirty_entities.lock();
        dirty_entities.insert(type_name.to_string());
    }

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

    pub fn set_negative_cache_enabled(&self, enabled: bool) {
        self.negative_cache_enabled.store(enabled, std::sync::atomic::Ordering::Release);
    }

    pub fn negative_cache_enabled(&self) -> bool {
        self.negative_cache_enabled.load(std::sync::atomic::Ordering::Acquire)
    }

    async fn load_entity_type(&self, _entity_type: &str) -> Result<()> {
        Ok(())
    }

    fn touch(&self) {
        *self.last_access.lock() = Instant::now();
    }

    pub fn player_id(&self) -> i64 {
        self.player_id
    }
}