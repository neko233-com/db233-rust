//! Entity definition and macros for db233-rust.
//!
//! This module provides traits and macros for defining database entities that can be
//! mapped to MySQL tables. The `DbEntity` trait defines the core interface for all entities,
//! including table name, primary key handling, and serialization requirements.
//!
//! Two macros are provided:
//! - `define_entity!`: Define a simple entity with inline primary key field.
//! - `define_entity_with_base!`: Define an entity with a `BaseEntity` that includes
//!   common fields like id, created_at, and updated_at.

use serde::{Deserialize, Serialize};
use std::any::Any;

/// Trait for database entities.
///
/// All entities must implement this trait to work with the ORM layer. It defines
/// the mapping between Rust structs and database tables, including table name,
/// primary key information, and serialization requirements.
pub trait DbEntity: Serialize + for<'de> Deserialize<'de> + Clone + Any + Send + Sync {
    /// Returns the database table name associated with this entity.
    fn table_name() -> &'static str;

    /// Returns the primary key column name in the database.
    fn primary_key_name() -> &'static str;

    /// Returns the current value of the primary key.
    fn primary_key_value(&self) -> i64;

    /// Sets the value of the primary key.
    ///
    /// This is typically called after inserting a new entity to set the auto-generated ID.
    fn set_primary_key_value(&mut self, value: i64);
}

/// Trait for entity metadata.
///
/// Provides methods for mapping between Rust field names and database column names.
/// This is useful for cases where field names and column names differ.
pub trait EntityMetadata {
    /// Gets the database column name for a given Rust field name.
    fn column_name(field_name: &str) -> Option<&'static str>;

    /// Gets the Rust field name for a given database column name.
    fn field_name(column_name: &str) -> Option<&'static str>;

    /// Returns all field-column mappings for this entity.
    fn columns() -> Vec<(&'static str, &'static str)>;
}

/// Base entity struct containing common fields.
///
/// This struct is intended to be embedded in other entity structs to provide
/// common database fields: id (primary key), created_at, and updated_at.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BaseEntity {
    /// Primary key ID.
    #[serde(rename = "id")]
    pub id: i64,

    /// Timestamp when the entity was created (Unix timestamp in seconds).
    #[serde(rename = "created_at")]
    pub created_at: i64,

    /// Timestamp when the entity was last updated (Unix timestamp in seconds).
    #[serde(rename = "updated_at")]
    pub updated_at: i64,
}

impl BaseEntity {
    /// Creates a new BaseEntity with current timestamp for created_at and updated_at.
    ///
    /// The id is set to 0, indicating a new entity that hasn't been persisted yet.
    pub fn new() -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: 0,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Macro to define a database entity with inline primary key.
///
/// This macro generates a struct with the specified fields and implements the `DbEntity` trait.
/// The primary key field `id` is automatically included in the struct.
///
/// # Usage
///
/// ```rust
/// use db233::define_entity;
///
/// define_entity!(User, "users", "id",
///     name: String => "name",
///     email: String => "email",
///     age: i32 => "age",
/// );
/// ```
///
/// # Parameters
///
/// - `$name`: The name of the Rust struct to generate.
/// - `$table`: The database table name (string literal).
/// - `$pk`: The primary key column name (string literal).
/// - `$($field: $ty => $col),*`: Field definitions where each field has a Rust type and
///   a corresponding database column name.
#[macro_export]
macro_rules! define_entity {
    ($name:ident, $table:expr, $pk:expr, $($field:ident: $ty:ty => $col:expr,)*) => {
        #[derive(
            Debug,
            Clone,
            $crate::serde::Serialize,
            $crate::serde::Deserialize,
            Default
        )]
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

/// Macro to define a database entity with embedded BaseEntity.
///
/// This macro generates a struct with a `base` field containing `BaseEntity` (id,
/// created_at, updated_at) plus the specified custom fields. It implements the `DbEntity` trait.
///
/// # Usage
///
/// ```rust
/// use db233::define_entity_with_base;
///
/// define_entity_with_base!(PlayerBaseEntity, "player_base",
///     name: String => "name",
///     level: i32 => "level",
///     exp: i64 => "exp",
/// );
/// ```
///
/// # Parameters
///
/// - `$name`: The name of the Rust struct to generate.
/// - `$table`: The database table name (string literal).
/// - `$($field: $ty => $col),*`: Field definitions where each field has a Rust type and
///   a corresponding database column name.
#[macro_export]
macro_rules! define_entity_with_base {
    ($name:ident, $table:expr, $($field:ident: $ty:ty => $col:expr,)*) => {
        #[derive(
            Debug,
            Clone,
            $crate::serde::Serialize,
            $crate::serde::Deserialize,
            Default
        )]
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
