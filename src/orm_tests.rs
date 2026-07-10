use crate::entity::DbEntity;
use crate::orm::OrmHandler;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TestEntity {
    pub id: i64,
    pub name: String,
    pub age: i32,
    pub status: String,
}

impl DbEntity for TestEntity {
    fn table_name() -> &'static str {
        "test_table"
    }

    fn primary_key_name() -> &'static str {
        "id"
    }

    fn primary_key_value(&self) -> i64 {
        self.id
    }

    fn set_primary_key_value(&mut self, value: i64) {
        self.id = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value;

    #[test]
    fn test_build_insert_sql() {
        let entity = TestEntity {
            id: 0,
            name: "test".to_string(),
            age: 25,
            status: "active".to_string(),
        };

        let (sql, values) = OrmHandler::build_insert_sql(&entity).unwrap();

        assert!(sql.starts_with("INSERT INTO test_table"));
        assert!(sql.contains("name"));
        assert!(sql.contains("age"));
        assert!(sql.contains("status"));
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_build_update_sql() {
        let entity = TestEntity {
            id: 1,
            name: "updated".to_string(),
            age: 30,
            status: "inactive".to_string(),
        };

        let (sql, values) = OrmHandler::build_update_sql(&entity).unwrap();

        assert!(sql.starts_with("UPDATE test_table"));
        assert!(sql.contains("WHERE id = ?"));
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn test_build_upsert_sql() {
        let entity = TestEntity {
            id: 1,
            name: "upsert".to_string(),
            age: 28,
            status: "active".to_string(),
        };

        let (sql, _values) = OrmHandler::build_upsert_sql(&entity).unwrap();

        assert!(sql.starts_with("INSERT INTO test_table"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn test_build_batch_upsert_sql() {
        let entities = vec![
            TestEntity {
                id: 1,
                name: "a".to_string(),
                age: 20,
                status: "active".to_string(),
            },
            TestEntity {
                id: 2,
                name: "b".to_string(),
                age: 30,
                status: "active".to_string(),
            },
        ];

        let (sql, values) = OrmHandler::build_batch_upsert_sql(&entities).unwrap();

        assert!(sql.contains("VALUES"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
        assert_eq!(values.len(), 8);
    }

    #[test]
    fn test_build_batch_upsert_empty() {
        let entities: Vec<TestEntity> = vec![];
        let result = OrmHandler::build_batch_upsert_sql(&entities);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_find_by_id_sql() {
        let (sql, values) = OrmHandler::build_find_by_id_sql::<TestEntity>(123);
        assert_eq!(sql, "SELECT * FROM test_table WHERE id = ?");
        assert_eq!(values, vec![Value::Int(123)]);
    }

    #[test]
    fn test_build_delete_sql() {
        let entity = TestEntity {
            id: 1,
            name: "test".to_string(),
            age: 25,
            status: "active".to_string(),
        };

        let (sql, values) = OrmHandler::build_delete_sql(&entity);
        assert_eq!(sql, "DELETE FROM test_table WHERE id = ?");
        assert_eq!(values, vec![Value::Int(1)]);
    }
}
