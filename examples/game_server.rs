use db233::{
    config::{CacheableEntitySpec, DbConnectionConfig, GameDbOptions},
    db::Db,
    entity::{BaseEntity, DbEntity},
    session::SessionRepository,
    define_entity_with_base,
};

define_entity_with_base!(PlayerBaseEntity, "player_base",
    name: String => "name",
    level: i32 => "level",
    exp: i64 => "exp",
);

define_entity_with_base!(PlayerBagEntity, "player_bag",
    gold: i64 => "gold",
    items: String => "items",
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = DbConnectionConfig::new(
        "127.0.0.1",
        3306,
        "root",
        "root",
        "db233_rust",
    );

    let mut db = Db::new(config.clone(), 1).await?;

    let _ = db.exec(
        r#"CREATE TABLE IF NOT EXISTS player_base (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            level INT DEFAULT 1,
            exp BIGINT DEFAULT 0,
            created_at BIGINT,
            updated_at BIGINT
        )"#,
        &[],
    ).await;

    let _ = db.exec(
        r#"CREATE TABLE IF NOT EXISTS player_bag (
            id BIGINT PRIMARY KEY,
            gold BIGINT DEFAULT 0,
            items TEXT,
            created_at BIGINT,
            updated_at BIGINT
        )"#,
        &[],
    ).await;

    let opts = GameDbOptions {
        enable_entity_cache: true,
        cacheable_entities: vec![
            CacheableEntitySpec::new("db233::examples::game_server::PlayerBaseEntity", 8000),
            CacheableEntitySpec::new("db233::examples::game_server::PlayerBagEntity", 8000),
        ],
        ..Default::default()
    };

    let session_repo = db.init_game_db(&config, opts).await?;

    let player_id = 1001;

    let session = session_repo.open_session(player_id, &[
        "db233::examples::game_server::PlayerBaseEntity",
        "db233::examples::game_server::PlayerBagEntity",
    ]).await?;

    let base = session.get_or_load::<PlayerBaseEntity>().await?;
    match base {
        Some(mut b) => {
            println!("Found existing player: {:?}", b);
            b.level += 1;
            session.put(&b).await?;
        }
        None => {
            let new_base = PlayerBaseEntity {
                base: BaseEntity::new(),
                name: format!("Player_{}", player_id),
                level: 1,
                exp: 0,
            };
            session.put(&new_base).await?;
            println!("Created new player");
        }
    }

    let bag = session.get_or_load::<PlayerBagEntity>().await?;
    match bag {
        Some(mut b) => {
            b.gold += 100;
            session.put(&b).await?;
            println!("Added gold to bag: {}", b.gold);
        }
        None => {
            let new_bag = PlayerBagEntity {
                base: BaseEntity::new(),
                gold: 1000,
                items: "[]".to_string(),
            };
            session.put(&new_bag).await?;
            println!("Created new bag");
        }
    }

    session.flush().await?;
    session_repo.close_session(player_id).await?;

    db.close().await?;

    Ok(())
}