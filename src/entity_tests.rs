use crate::entity::{BaseEntity, DbEntity};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SimpleEntity {
    pub id: i64,
    pub name: String,
}

impl DbEntity for SimpleEntity {
    fn table_name() -> &'static str {
        "simple"
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
    use crate::define_entity;
    use crate::define_entity_with_base;

    #[test]
    fn test_base_entity_new() {
        let base = BaseEntity::new();
        assert_eq!(base.id, 0);
        assert!(base.created_at > 0);
        assert_eq!(base.created_at, base.updated_at);
    }

    #[test]
    fn test_base_entity_default() {
        let base: BaseEntity = Default::default();
        assert_eq!(base.id, 0);
        assert_eq!(base.created_at, 0);
        assert_eq!(base.updated_at, 0);
    }

    #[test]
    fn test_entity_primary_key() {
        let mut entity = SimpleEntity {
            id: 123,
            name: "test".to_string(),
        };

        assert_eq!(entity.primary_key_value(), 123);

        entity.set_primary_key_value(456);
        assert_eq!(entity.id, 456);
        assert_eq!(entity.primary_key_value(), 456);
    }

    #[test]
    fn test_entity_table_name() {
        assert_eq!(SimpleEntity::table_name(), "simple");
        assert_eq!(SimpleEntity::primary_key_name(), "id");
    }

    #[test]
    fn test_entity_serde() {
        let entity = SimpleEntity {
            id: 1,
            name: "test".to_string(),
        };

        let json = serde_json::to_string(&entity).unwrap();
        let deserialized: SimpleEntity = serde_json::from_str(&json).unwrap();

        assert_eq!(entity.id, deserialized.id);
        assert_eq!(entity.name, deserialized.name);
    }

    #[test]
    fn test_define_entity_macro() {
        define_entity!(TestMacroEntity, "test_macro", "id",
            name: String => "name",
            value: i32 => "value",
        );

        let mut entity = TestMacroEntity {
            id: 0,
            name: "macro_test".to_string(),
            value: 42,
        };

        assert_eq!(TestMacroEntity::table_name(), "test_macro");
        assert_eq!(TestMacroEntity::primary_key_name(), "id");
        assert_eq!(entity.primary_key_value(), 0);

        entity.set_primary_key_value(100);
        assert_eq!(entity.id, 100);
    }

    #[test]
    fn test_define_entity_with_base_macro() {
        define_entity_with_base!(TestWithBaseEntity, "test_with_base",
            name: String => "name",
            value: i32 => "value",
        );

        let mut entity = TestWithBaseEntity {
            base: BaseEntity::new(),
            name: "base_test".to_string(),
            value: 99,
        };

        assert_eq!(TestWithBaseEntity::table_name(), "test_with_base");
        assert_eq!(TestWithBaseEntity::primary_key_name(), "id");
        assert_eq!(entity.primary_key_value(), 0);

        entity.set_primary_key_value(200);
        assert_eq!(entity.base.id, 200);
    }
}
