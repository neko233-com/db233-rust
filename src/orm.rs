//! ORM (Object-Relational Mapping) layer for db233-rust.
//!
//! This module provides the core ORM functionality, including SQL generation for
//! INSERT, UPDATE, DELETE, UPSERT, and batch operations. It also handles converting
//! between database rows and Rust entities via JSON serialization.
//!
//! Key components:
//! - `OrmHandler`: Static methods for building SQL queries and mapping rows to entities.
//! - `FromRowJson`: Trait for converting database rows to Rust entities.
//! - Helper functions for converting between MySQL values and JSON values.

use crate::entity::DbEntity;
use crate::error::{Db233Error, Result};
use base64::{engine::general_purpose, Engine};
use mysql_async::Row;
use serde::de::DeserializeOwned;

/// ORM handler for SQL generation and entity mapping.
///
/// Provides static methods for building SQL queries and converting database rows
/// to Rust entities.
pub struct OrmHandler;

impl OrmHandler {
    /// Converts database rows to a vector of entities.
    ///
    /// # Parameters
    ///
    /// - `rows`: Vector of database rows to convert.
    ///
    /// # Returns
    ///
    /// Returns a vector of deserialized entities, or an error if mapping fails.
    pub async fn query_to_entity<T>(rows: Vec<Row>) -> Result<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let mut results = Vec::new();
        for row in rows {
            let json_val = row_to_json(&row);
            let entity: T = serde_json::from_value(json_val)
                .map_err(|e| Db233Error::MappingError(e.to_string()))?;
            results.push(entity);
        }
        Ok(results)
    }

    /// Converts database rows to a vector of HashMaps.
    ///
    /// Each row is converted to a HashMap where keys are column names and values
    /// are the corresponding MySQL values.
    ///
    /// # Parameters
    ///
    /// - `rows`: Vector of database rows to convert.
    ///
    /// # Returns
    ///
    /// Returns a vector of HashMaps representing the rows.
    pub async fn query_to_map(
        rows: Vec<Row>,
    ) -> Result<Vec<std::collections::HashMap<String, mysql_async::Value>>> {
        let mut results = Vec::new();
        for row in rows {
            let columns = row.columns();
            let mut row_map = std::collections::HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                let value = row.get(i).unwrap_or(mysql_async::Value::NULL);
                row_map.insert(col.name_str().to_string(), value);
            }
            results.push(row_map);
        }
        Ok(results)
    }

    /// Builds an INSERT SQL statement for an entity.
    ///
    /// Excludes the primary key if it's 0 (indicating a new entity).
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to insert.
    ///
    /// # Returns
    ///
    /// Returns a tuple of (SQL statement, parameter values).
    pub fn build_insert_sql<T: DbEntity>(entity: &T) -> Result<(String, Vec<mysql_async::Value>)> {
        let json_str =
            serde_json::to_string(entity).map_err(|e| Db233Error::MappingError(e.to_string()))?;
        let json_val: serde_json::Value =
            serde_json::from_str(&json_str).map_err(|e| Db233Error::MappingError(e.to_string()))?;

        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut values = Vec::new();

        if let serde_json::Value::Object(obj) = json_val {
            for (key, value) in obj {
                if key == "id" && value.as_i64() == Some(0) {
                    continue;
                }
                columns.push(key.clone());
                placeholders.push("?");
                values.push(convert_json_value(value));
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            T::table_name(),
            columns.join(", "),
            placeholders.join(", ")
        );

        Ok((sql, values))
    }

    /// Builds an UPDATE SQL statement for an entity.
    ///
    /// Excludes the primary key and created_at field from the SET clause.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to update.
    ///
    /// # Returns
    ///
    /// Returns a tuple of (SQL statement, parameter values).
    pub fn build_update_sql<T: DbEntity>(entity: &T) -> Result<(String, Vec<mysql_async::Value>)> {
        let json_str =
            serde_json::to_string(entity).map_err(|e| Db233Error::MappingError(e.to_string()))?;
        let json_val: serde_json::Value =
            serde_json::from_str(&json_str).map_err(|e| Db233Error::MappingError(e.to_string()))?;

        let mut set_clause = Vec::new();
        let mut values = Vec::new();

        if let serde_json::Value::Object(obj) = json_val {
            for (key, value) in obj {
                if key == "id" {
                    continue;
                }
                if key == "created_at" {
                    continue;
                }
                set_clause.push(format!("{} = ?", key));
                values.push(convert_json_value(value));
            }
        }

        let pk_name = T::primary_key_name();
        let pk_value = entity.primary_key_value();
        values.push(mysql_async::Value::Int(pk_value));

        let sql = format!(
            "UPDATE {} SET {} WHERE {} = ?",
            T::table_name(),
            set_clause.join(", "),
            pk_name
        );

        Ok((sql, values))
    }

    /// Builds an UPSERT SQL statement for an entity.
    ///
    /// Uses MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` syntax.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to upsert.
    ///
    /// # Returns
    ///
    /// Returns a tuple of (SQL statement, parameter values).
    pub fn build_upsert_sql<T: DbEntity>(entity: &T) -> Result<(String, Vec<mysql_async::Value>)> {
        let json_str =
            serde_json::to_string(entity).map_err(|e| Db233Error::MappingError(e.to_string()))?;
        let json_val: serde_json::Value =
            serde_json::from_str(&json_str).map_err(|e| Db233Error::MappingError(e.to_string()))?;

        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut update_clause = Vec::new();
        let mut values = Vec::new();

        if let serde_json::Value::Object(obj) = json_val {
            for (key, value) in obj {
                if key == "id" && value.as_i64() == Some(0) {
                    continue;
                }
                columns.push(key.clone());
                placeholders.push("?");
                if key != "id" && key != "created_at" {
                    update_clause.push(format!("{} = VALUES({})", key, key));
                }
                values.push(convert_json_value(value));
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}",
            T::table_name(),
            columns.join(", "),
            placeholders.join(", "),
            update_clause.join(", ")
        );

        Ok((sql, values))
    }

    /// Builds a batch UPSERT SQL statement for multiple entities.
    ///
    /// Uses MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` syntax for batch operations.
    ///
    /// # Parameters
    ///
    /// - `entities`: The entities to upsert in batch.
    ///
    /// # Returns
    ///
    /// Returns a tuple of (SQL statement, parameter values).
    ///
    /// # Errors
    ///
    /// Returns an error if the entities list is empty.
    pub fn build_batch_upsert_sql<T: DbEntity>(
        entities: &[T],
    ) -> Result<(String, Vec<mysql_async::Value>)> {
        if entities.is_empty() {
            return Err(Db233Error::ParameterError(
                "empty entities list".to_string(),
            ));
        }

        let json_str = serde_json::to_string(&entities[0])
            .map_err(|e| Db233Error::MappingError(e.to_string()))?;
        let json_val: serde_json::Value =
            serde_json::from_str(&json_str).map_err(|e| Db233Error::MappingError(e.to_string()))?;

        let mut columns = Vec::new();
        let mut update_clause = Vec::new();

        if let serde_json::Value::Object(obj) = json_val {
            for (key, value) in obj {
                if key == "id" && value.as_i64() == Some(0) {
                    continue;
                }
                columns.push(key.clone());
                if key != "id" && key != "created_at" {
                    update_clause.push(format!("{} = VALUES({})", key, key));
                }
            }
        }

        let mut values = Vec::new();
        let mut rows_sql = Vec::new();

        for entity in entities {
            let json_str = serde_json::to_string(entity)
                .map_err(|e| Db233Error::MappingError(e.to_string()))?;
            let json_val: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| Db233Error::MappingError(e.to_string()))?;

            if let serde_json::Value::Object(obj) = json_val {
                let mut row_values = Vec::new();
                for col in &columns {
                    if let Some(value) = obj.get(col) {
                        row_values.push(convert_json_value(value.clone()));
                    }
                }
                values.extend(row_values);
                rows_sql.push(format!(
                    "({})",
                    "?,".repeat(columns.len()).trim_end_matches(',')
                ));
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
            T::table_name(),
            columns.join(", "),
            rows_sql.join(", "),
            update_clause.join(", ")
        );

        Ok((sql, values))
    }

    /// Builds a SELECT SQL statement to find an entity by ID.
    ///
    /// # Parameters
    ///
    /// - `id`: The primary key value of the entity to find.
    ///
    /// # Returns
    ///
    /// Returns a tuple of (SQL statement, parameter values).
    pub fn build_find_by_id_sql<T: DbEntity>(id: i64) -> (String, Vec<mysql_async::Value>) {
        let sql = format!(
            "SELECT * FROM {} WHERE {} = ?",
            T::table_name(),
            T::primary_key_name()
        );
        let values = vec![mysql_async::Value::Int(id)];
        (sql, values)
    }

    /// Builds a DELETE SQL statement for an entity.
    ///
    /// # Parameters
    ///
    /// - `entity`: The entity to delete.
    ///
    /// # Returns
    ///
    /// Returns a tuple of (SQL statement, parameter values).
    pub fn build_delete_sql<T: DbEntity>(entity: &T) -> (String, Vec<mysql_async::Value>) {
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            T::table_name(),
            T::primary_key_name()
        );
        let values = vec![mysql_async::Value::Int(entity.primary_key_value())];
        (sql, values)
    }
}

/// Converts a JSON value to a MySQL value.
///
/// Handles all JSON value types (Null, Bool, Number, String, Array, Object).
fn convert_json_value(value: serde_json::Value) -> mysql_async::Value {
    match value {
        serde_json::Value::Null => mysql_async::Value::NULL,
        serde_json::Value::Bool(b) => mysql_async::Value::Int(if b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                mysql_async::Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                mysql_async::Value::UInt(u)
            } else if let Some(f) = n.as_f64() {
                mysql_async::Value::Float(f as f32)
            } else {
                mysql_async::Value::NULL
            }
        }
        serde_json::Value::String(s) => mysql_async::Value::Bytes(s.as_bytes().to_vec()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let json_str = serde_json::to_string(&value).unwrap_or_default();
            mysql_async::Value::Bytes(json_str.as_bytes().to_vec())
        }
    }
}

/// Trait for converting database rows to Rust entities via JSON.
///
/// Provides a default implementation that converts a row to JSON and then
/// deserializes it into the target type.
pub trait FromRowJson: DeserializeOwned + Sized {
    /// Converts a database row to an entity.
    ///
    /// # Parameters
    ///
    /// - `row`: The database row to convert.
    ///
    /// # Returns
    ///
    /// Returns the deserialized entity, or an error if conversion fails.
    fn from_row_json(row: &Row) -> Result<Self> {
        let json_val = row_to_json(row);
        serde_json::from_value(json_val).map_err(|e| Db233Error::MappingError(e.to_string()))
    }
}

/// Converts a database row to a JSON object.
///
/// Maps column names to their values, converting MySQL values to appropriate
/// JSON types.
fn row_to_json(row: &Row) -> serde_json::Value {
    let columns = row.columns();
    let mut map = serde_json::Map::new();

    for (i, col) in columns.iter().enumerate() {
        let value = row.get(i).unwrap_or(mysql_async::Value::NULL);
        let json_value = convert_mysql_value(value);
        map.insert(col.name_str().to_string(), json_value);
    }

    serde_json::Value::Object(map)
}

/// Converts a MySQL value to a JSON value.
///
/// Handles all MySQL value types, including NULL, Int, UInt, Float, Double,
/// Date, Time, and Bytes.
fn convert_mysql_value(value: mysql_async::Value) -> serde_json::Value {
    match value {
        mysql_async::Value::NULL => serde_json::Value::Null,
        mysql_async::Value::Int(i) => serde_json::Value::Number(i.into()),
        mysql_async::Value::UInt(u) => serde_json::Value::Number(u.into()),
        mysql_async::Value::Float(f) => {
            serde_json::Value::Number(serde_json::Number::from_f64(f.into()).unwrap())
        }
        mysql_async::Value::Double(f) => {
            serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap())
        }
        mysql_async::Value::Date(y, m, d, hh, mm, ss, _) => serde_json::Value::String(format!(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}",
            y, m, d, hh, mm, ss
        )),
        mysql_async::Value::Time(_neg, _days, hh, mm, ss, _) => {
            serde_json::Value::String(format!("{:02}:{:02}:{:02}", hh, mm, ss))
        }
        mysql_async::Value::Bytes(b) => {
            if let Ok(s) = String::from_utf8(b.clone()) {
                serde_json::Value::String(s)
            } else {
                serde_json::Value::String(general_purpose::STANDARD.encode(&b))
            }
        }
    }
}
