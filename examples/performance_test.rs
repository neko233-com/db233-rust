use db233::{
    config::DbConnectionConfig,
    db::Db,
    entity::{BaseEntity, DbEntity},
    define_entity_with_base,
};
use rand::Rng;
use std::time::{Duration, Instant};

define_entity_with_base!(PerformanceTestEntity, "performance_test",
    name: String => "name",
    score: i64 => "score",
    data: String => "data",
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

    let mut db = Db::new(config, 1).await?;

    let _ = db.exec(
        r#"CREATE TABLE IF NOT EXISTS performance_test (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            score BIGINT DEFAULT 0,
            data TEXT,
            created_at BIGINT,
            updated_at BIGINT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#,
        &[],
    ).await;

    let _ = db.exec("TRUNCATE TABLE performance_test", &[]).await;

    let batch_size = 1000;
    let total_records = 10000;

    println!("=== Performance Test ===");
    println!("Batch size: {}", batch_size);
    println!("Total records: {}", total_records);
    println!();

    let start = Instant::now();
    for i in 0..(total_records / batch_size) {
        let mut entities = Vec::with_capacity(batch_size);
        for j in 0..batch_size {
            let id = (i * batch_size + j + 1) as i64;
            entities.push(PerformanceTestEntity {
                base: BaseEntity {
                    id,
                    created_at: 0,
                    updated_at: 0,
                },
                name: format!("Player_{}", id),
                score: rand::thread_rng().gen_range(0..1_000_000),
                data: format!("{{\"level\":{},\"items\":[]}}", rand::thread_rng().gen_range(1..100)),
            });
        }
        db.save_batch_upsert(&entities).await?;
        println!("Inserted batch {} of {} ({:.1}%)", i + 1, total_records / batch_size, ((i + 1) * 100) as f64 / ((total_records / batch_size) as f64));
    }
    let elapsed = start.elapsed();
    println!();
    println!("Batch insert time: {:?}", elapsed);
    println!("Throughput: {:.1} records/sec", total_records as f64 / elapsed.as_secs_f64());
    println!();

    let start = Instant::now();
    let ids: Vec<i64> = (1..=100).collect();
    let results = db.find_by_ids::<PerformanceTestEntity>(&ids).await?;
    let elapsed = start.elapsed();
    println!("Find by IDs (100) time: {:?}", elapsed);
    println!("Found {} records", results.len());
    println!();

    let start = Instant::now();
    let _ = db.query_to_int64("SELECT COUNT(*) FROM performance_test", &[]).await?;
    let elapsed = start.elapsed();
    println!("Count query time: {:?}", elapsed);
    println!();

    let start = Instant::now();
    let _ = db.exec("UPDATE performance_test SET score = score + 1", &[]).await?;
    let elapsed = start.elapsed();
    println!("Update all time: {:?}", elapsed);
    println!();

    let start = Instant::now();
    let params = vec![(1i64..=100).map(|id| (id, id + 1)).collect::<Vec<_>>()];
    let elapsed = start.elapsed();
    println!("Batch update prep time: {:?}", elapsed);
    println!();

    let start = Instant::now();
    for i in 1..=100 {
        let mut entity = PerformanceTestEntity {
            base: BaseEntity {
                id: i,
                created_at: 0,
                updated_at: 0,
            },
            name: format!("Player_{}_updated", i),
            score: i as i64 * 100,
            data: "{}".to_string(),
        };
        db.update(&entity).await?;
    }
    let elapsed = start.elapsed();
    println!("100 single updates time: {:?}", elapsed);
    println!("Throughput: {:.1} updates/sec", 100f64 / elapsed.as_secs_f64());
    println!();

    db.close().await?;

    println!("=== Test Complete ===");

    Ok(())
}
