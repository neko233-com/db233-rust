use db233::{
    config::DbConnectionConfig,
    db::Db,
    define_entity_with_base,
    entity::{BaseEntity, DbEntity},
    mysql_async::Value,
};

define_entity_with_base!(User, "users",
    name: String => "name",
    email: String => "email",
    age: i32 => "age",
    status: String => "status",
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = DbConnectionConfig::new("127.0.0.1", 3306, "root", "root", "db233_rust");

    let mut db = Db::new(config, 1).await?;

    let _ = db
        .exec(
            r#"CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE,
            age INT,
            status VARCHAR(32) DEFAULT 'active',
            created_at BIGINT,
            updated_at BIGINT
        )"#,
            &[],
        )
        .await;

    let mut user = User {
        base: BaseEntity::new(),
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        age: 25,
        status: "active".to_string(),
    };

    db.save(&mut user).await?;
    println!("Saved user with ID: {}", user.base.id);

    let found = db.find_by_id::<User>(user.base.id).await;
    println!("Found user: {:?}", found);

    user.name = "Alice Smith".to_string();
    db.update(&user).await?;
    println!("Updated user: {:?}", user);

    let count = db.query_to_int64("SELECT COUNT(*) FROM users", &[]).await?;
    println!("Total users: {}", count);

    let mut params = std::collections::HashMap::new();
    params.insert("minAge".to_string(), Value::Int(18));
    params.insert(
        "status".to_string(),
        Value::Bytes("active".as_bytes().to_vec()),
    );

    let rows = db
        .query_named(
            "SELECT * FROM users WHERE age > {minAge} AND status={status}",
            &params,
        )
        .await?;
    println!("Query named results: {:?}", rows);

    let ids = db
        .query_named_to_int64_slice("SELECT id FROM users WHERE status={status}", &params)
        .await?;
    println!("User IDs: {:?}", ids);

    db.close().await?;

    Ok(())
}
