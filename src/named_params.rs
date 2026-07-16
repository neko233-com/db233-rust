//! Named parameter handling for SQL queries.
//!
//! This module provides functions to convert SQL queries with named parameters
//! (e.g., `{param_name}`) into standard parameterized queries with positional
//! placeholders (`?`). This improves SQL readability and maintainability.

use crate::error::{Db233Error, Result};
use mysql_async::Value;
use std::collections::HashMap;

/// Replaces named parameters in a SQL query with positional placeholders.
///
/// Converts queries like `SELECT * FROM users WHERE id={userId}` into
/// `SELECT * FROM users WHERE id=?` and builds a corresponding vector of values.
///
/// # Parameters
///
/// - `sql`: The SQL query with named parameters in `{param_name}` format.
/// - `params`: A HashMap mapping parameter names to their values.
///
/// # Returns
///
/// Returns a tuple of (processed SQL query, parameter values in order).
///
/// # Errors
///
/// Returns an error if:
/// - A placeholder is not properly closed (missing `}`).
/// - A required parameter is missing from the params HashMap.
pub fn replace_named_parameters(
    sql: &str,
    params: &HashMap<String, Value>,
) -> Result<(String, Vec<Value>)> {
    let mut new_sql = String::with_capacity(sql.len());
    let mut values = Vec::new();
    let mut i = 0;

    while i < sql.len() {
        let start_idx = match sql[i..].find('{') {
            Some(idx) => i + idx,
            None => {
                new_sql.push_str(&sql[i..]);
                break;
            }
        };

        new_sql.push_str(&sql[i..start_idx]);

        let end_idx = match sql[start_idx + 1..].find('}') {
            Some(idx) => start_idx + 1 + idx,
            None => {
                return Err(Db233Error::ParameterError(format!(
                    "unclosed placeholder: {}",
                    &sql[start_idx..]
                )));
            }
        };

        let param_name = &sql[start_idx + 1..end_idx];
        let value = params.get(param_name).ok_or_else(|| {
            Db233Error::ParameterError(format!("missing required parameter: {}", param_name))
        })?;

        new_sql.push('?');
        values.push(value.clone());

        i = end_idx + 1;
    }

    Ok((new_sql, values))
}

/// Builds batch named parameters for multiple rows.
///
/// Generates the processed SQL query once and builds a vector of parameter vectors,
/// one for each row in the batch.
///
/// # Parameters
///
/// - `sql`: The SQL query with named parameters in `{param_name}` format.
/// - `params_list`: A slice of HashMaps, each representing parameters for one row.
///
/// # Returns
///
/// Returns a tuple of (processed SQL query, vector of parameter value vectors).
pub fn build_batch_named_params(
    sql: &str,
    params_list: &[HashMap<String, Value>],
) -> Result<(String, Vec<Vec<Value>>)> {
    if params_list.is_empty() {
        return Ok((sql.to_string(), Vec::new()));
    }

    let first_params = &params_list[0];
    let (new_sql, _) = replace_named_parameters(sql, first_params)?;

    let mut batch_values = Vec::with_capacity(params_list.len());
    for params in params_list {
        let (_, values) = replace_named_parameters(sql, params)?;
        batch_values.push(values);
    }

    Ok((new_sql, batch_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value;

    #[test]
    fn test_replace_named_parameters() {
        let sql = "SELECT * FROM users WHERE id={userId} AND status={status}";
        let mut params = HashMap::new();
        params.insert("userId".to_string(), Value::Int(123));
        params.insert(
            "status".to_string(),
            Value::Bytes("active".as_bytes().to_vec()),
        );

        let (new_sql, values) = replace_named_parameters(sql, &params).unwrap();

        assert_eq!(new_sql, "SELECT * FROM users WHERE id=? AND status=?");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Int(123));
    }

    #[test]
    fn test_missing_parameter() {
        let sql = "SELECT * FROM users WHERE id={userId}";
        let params = HashMap::new();

        let result = replace_named_parameters(sql, &params);
        assert!(result.is_err());
    }

    #[test]
    fn test_unclosed_placeholder() {
        let sql = "SELECT * FROM users WHERE id={userId";
        let mut params = HashMap::new();
        params.insert("userId".to_string(), Value::Int(123));

        let result = replace_named_parameters(sql, &params);
        assert!(result.is_err());
    }
}
