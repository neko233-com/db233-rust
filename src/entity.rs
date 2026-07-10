use serde::{Deserialize, Serialize};
use std::any::Any;

pub trait DbEntity: Serialize + for<'de> Deserialize<'de> + Clone + Any + Send + Sync {
    fn table_name() -> &'static str;
    fn primary_key_name() -> &'static str;
    fn primary_key_value(&self) -> i64;
    fn set_primary_key_value(&mut self, value: i64);
}

pub trait EntityMetadata {
    fn column_name(field_name: &str) -> Option<&'static str>;
    fn field_name(column_name: &str) -> Option<&'static str>;
    fn columns() -> Vec<(&'static str, &'static str)>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BaseEntity {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "created_at")]
    pub created_at: i64,
    #[serde(rename = "updated_at")]
    pub updated_at: i64,
}

impl BaseEntity {
    pub fn new() -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: 0,
            created_at: now,
            updated_at: now,
        }
    }
}

#[macro_export]
macro_rules! define_entity {
    ($name:ident, $table:expr, $pk:expr, $($field:ident: $ty:ty => $col:expr,)*) => {
        #[derive(Debug, Clone, Serialize, Deserialize, Default)]
        pub struct $name {
            pub id: i64,
            $(pub $field: $ty,)*
        }

        impl $crate::entity::DbEntity for $name {
            fn table_name() -> &'static str {
                $table
            }

            fn primary_key_name() -> &'static str {
                $pk
            }

            fn primary_key_value(&self) -> i64 {
                self.id
            }

            fn set_primary_key_value(&mut self, value: i64) {
                self.id = value;
            }
        }
    };
}

#[macro_export]
macro_rules! define_entity_with_base {
    ($name:ident, $table:expr, $($field:ident: $ty:ty => $col:expr,)*) => {
        #[derive(Debug, Clone, Serialize, Deserialize, Default)]
        pub struct $name {
            pub base: $crate::entity::BaseEntity,
            $(pub $field: $ty,)*
        }

        impl $crate::entity::DbEntity for $name {
            fn table_name() -> &'static str {
                $table
            }

            fn primary_key_name() -> &'static str {
                "id"
            }

            fn primary_key_value(&self) -> i64 {
                self.base.id
            }

            fn set_primary_key_value(&mut self, value: i64) {
                self.base.id = value;
            }
        }
    };
}