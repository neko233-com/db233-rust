# db233-rust

Rust ORM / Database library for stateful game servers with high QPS MySQL.

This is a Rust version of [db233-go](https://github.com/neko233-com/db233-go), optimized specifically for Rust's async runtime and memory safety.

## Features

- **High Performance**: Optimized for high QPS game server workloads
- **Session L1 Cache**: In-memory caching for active player sessions using LRU
- **Batch UPSERT**: Efficient bulk insert/update operations
- **Write-Ahead Logging (WAL)**: Data durability guarantee
- **Connection Pool**: Configurable connection pooling with health checks
- **Named Parameter Queries**: SQL queries with named placeholders
- **Entity Mapping**: Macros for easy entity definition
- **Async Runtime**: Built on tokio for non-blocking operations
- **Plugin System**: Extensible plugin architecture
- **Monitoring**: Connection pool metrics and performance statistics

## Installation

```toml
[dependencies]
db233 = "0.1"
```

## Quick Start

```rust
use db233::{
    config::DbConnectionConfig,
    db::Db,
    entity::{BaseEntity, DbEntity},
    define_entity_with_base,
};

define_entity_with_base!(User, "users",
    name: String => "name",
    email: String => "email",
    age: i32 => "age",
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DbConnectionConfig::new("127.0.0.1", 3306, "root", "password", "game_db");
    let mut db = Db::new(config, 1).await?;

    let mut user = User {
        base: BaseEntity::new(),
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        age: 25,
    };

    db.save(&mut user).await?;
    println!("Saved user with ID: {}", user.base.id);

    let found = db.find_by_id::<User>(user.base.id).await?;
    println!("Found user: {:?}", found);

    db.close().await?;
    Ok(())
}
```

## Configuration

### Connection Config

```rust
let config = DbConnectionConfig {
    host: "127.0.0.1".to_string(),
    port: 3306,
    username: "root".to_string(),
    password: "password".to_string(),
    database: "game_db".to_string(),
    max_open_conns: 100,
    max_idle_conns: 20,
    conn_max_lifetime_sec: 3600,
    conn_max_idle_time_sec: 600,
};
```

### Performance Config (JSON)

```json
{
  "concurrent_max_workers": 16,
  "batch_upsert_chunk_size": 200,
  "write_buffer_enabled": true,
  "write_buffer_flush_interval_ms": 100,
  "max_open_conns": 100,
  "max_idle_conns": 20,
  "enable_local_journal": true,
  "local_journal_path": "./data/db233_journal",
  "entity_cache": {
    "enabled": true,
    "eviction_policy": "lru",
    "max_sessions": 10000,
    "session_flush_interval_ms": 60000,
    "flush_on_evict": true
  }
}
```

## Advanced Usage

### Named Parameters

```rust
let mut params = HashMap::new();
params.insert("minAge".to_string(), Value::Int(18));
params.insert("status".to_string(), Value::Bytes("active".as_bytes().to_vec()));

let rows = db.query_named(
    "SELECT * FROM users WHERE age > {minAge} AND status={status}",
    &params
).await?;
```

### Session Cache

```rust
let opts = GameDbOptions {
    enable_entity_cache: true,
    cacheable_entities: vec![
        CacheableEntitySpec::new("PlayerBaseEntity", 8000),
        CacheableEntitySpec::new("PlayerBagEntity", 8000),
    ],
    ..Default::default()
};

let session_repo = db.init_game_db(&config, opts).await?;
let session = session_repo.open_session(player_id, &["PlayerBaseEntity"]).await?;

let base = session.get_or_load::<PlayerBaseEntity>().await?;
```

### Batch UPSERT

```rust
let entities = vec![user1, user2, user3];
db.save_batch_upsert(&entities).await?;
```

### Write-Ahead Logging

```rust
db.enable_wal("./data/journal").await?;
```

## License

Apache-2.0
