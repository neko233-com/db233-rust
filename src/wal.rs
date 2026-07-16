//! Write-Ahead Logging (WAL) for db233-rust.
//!
//! This module provides a local Write-Ahead Log implementation for data durability.
//! Before database writes are committed, they are logged to disk in a structured
//! format (NDJSON). This ensures that in case of a crash or power failure,
//! uncommitted writes can be replayed to maintain data integrity.
//!
//! Key components:
//! - `LocalWriteJournal`: Main WAL implementation that writes to local filesystem.
//! - `WriteEntry`: Struct representing a single write operation in the log.
//! - `ValueSerde`: Serializable wrapper for MySQL values.
//!
//! Features:
//! - NDJSON format for easy parsing and recovery
//! - Automatic flushing after each write for durability
//! - Replay functionality for crash recovery
//! - Separate pending file for incomplete writes

use crate::error::{Db233Error, Result};
use chrono;
use mysql_async::Value;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A single write entry in the WAL log.
///
/// Contains the timestamp, SQL statement, and parameters for a write operation.
/// Stored in NDJSON format for easy parsing during recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteEntry {
    /// Unix timestamp in seconds when the write was recorded.
    pub timestamp: i64,
    /// The SQL statement that was executed.
    pub sql: String,
    /// The parameters bound to the SQL statement.
    pub params: Vec<ValueSerde>,
}

/// Serializable wrapper for MySQL Value types.
///
/// Provides a serde-compatible representation of mysql_async::Value for
/// serialization to the WAL log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueSerde {
    /// NULL value.
    Null,
    /// Signed integer value.
    Int(i64),
    /// Unsigned integer value.
    UInt(u64),
    /// Floating-point value (supports both Float and Double).
    Float(f64),
    /// Binary data or string value.
    Bytes(Vec<u8>),
}

impl From<Value> for ValueSerde {
    /// Converts a mysql_async::Value to a ValueSerde for serialization.
    ///
    /// Handles all MySQL value types, converting Date and Time to string representations
    /// and normalizing Float to Double.
    fn from(value: Value) -> Self {
        match value {
            Value::NULL => ValueSerde::Null,
            Value::Int(i) => ValueSerde::Int(i),
            Value::UInt(u) => ValueSerde::UInt(u),
            Value::Float(f) => ValueSerde::Float(f.into()),
            Value::Double(f) => ValueSerde::Float(f),
            Value::Date(y, m, d, hh, mm, ss, _) => ValueSerde::Bytes(
                format!("{}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss).into_bytes(),
            ),
            Value::Time(_neg, _days, hh, mm, ss, _) => {
                ValueSerde::Bytes(format!("{:02}:{:02}:{:02}", hh, mm, ss).into_bytes())
            }
            Value::Bytes(b) => ValueSerde::Bytes(b),
        }
    }
}

impl From<ValueSerde> for Value {
    /// Converts a ValueSerde back to mysql_async::Value for database operations.
    ///
    /// Used during WAL replay to reconstruct the original parameter values.
    fn from(value: ValueSerde) -> Self {
        match value {
            ValueSerde::Null => Value::NULL,
            ValueSerde::Int(i) => Value::Int(i),
            ValueSerde::UInt(u) => Value::UInt(u),
            ValueSerde::Float(f) => Value::Float(f as f32),
            ValueSerde::Bytes(b) => Value::Bytes(b),
        }
    }
}

/// Local Write-Ahead Log implementation.
///
/// Writes database operations to a local NDJSON file for durability. Each write
/// is flushed to disk immediately to ensure it survives crashes. Supports
/// replay of logged operations for recovery.
pub struct LocalWriteJournal {
    /// Path to the directory containing WAL files.
    path: String,
    /// Buffered writer for the main journal file.
    writer: Arc<Mutex<BufWriter<File>>>,
    /// Atomic flag controlling whether writes are accepted.
    running: AtomicBool,
    /// Path to the pending file for incomplete writes.
    pending_file: String,
}

impl LocalWriteJournal {
    /// Creates a new LocalWriteJournal.
    ///
    /// Creates the directory if it doesn't exist and opens the journal file
    /// in append mode. If the journal file doesn't exist, it will be created.
    ///
    /// # Parameters
    ///
    /// - `path`: Path to the directory where WAL files will be stored.
    ///
    /// # Returns
    ///
    /// Returns the new LocalWriteJournal, or an error if initialization fails.
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_err(|e| Db233Error::WalError(e.to_string()))?;

        let journal_path = Path::new(path).join("journal.ndjson");
        let pending_path = Path::new(path).join("pending.ndjson");

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .map_err(|e| Db233Error::WalError(e.to_string()))?;

        Ok(Self {
            path: path.to_string(),
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
            running: AtomicBool::new(true),
            pending_file: pending_path.to_string_lossy().to_string(),
        })
    }

    /// Records a write operation to the WAL.
    ///
    /// Serializes the SQL statement and parameters to a WriteEntry, writes it to
    /// the journal file as a JSON line, and immediately flushes to disk.
    ///
    /// # Parameters
    ///
    /// - `sql`: The SQL statement to record.
    /// - `params`: The parameters bound to the SQL statement.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success, or an error if writing fails.
    pub async fn record_write(&self, sql: &str, params: &[Value]) -> Result<()> {
        if !self.running.load(Ordering::Acquire) {
            return Ok(());
        }

        let entry = WriteEntry {
            timestamp: chrono::Utc::now().timestamp(),
            sql: sql.to_string(),
            params: params.iter().map(|p| (*p).clone().into()).collect(),
        };

        let json_str =
            serde_json::to_string(&entry).map_err(|e| Db233Error::WalError(e.to_string()))?;

        let mut writer = self.writer.lock().await;
        writeln!(writer, "{}", json_str).map_err(|e| Db233Error::WalError(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| Db233Error::WalError(e.to_string()))?;

        Ok(())
    }

    /// Stops the WAL and flushes any pending writes.
    ///
    /// Sets the running flag to false and flushes the writer to ensure all
    /// pending writes are persisted to disk.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::Release);

        let mut writer = self.writer.lock().await;
        writer
            .flush()
            .map_err(|e| Db233Error::WalError(e.to_string()))?;

        Ok(())
    }

    /// Replays all entries from the main journal file.
    ///
    /// Reads the journal file line by line, deserializing each JSON entry.
    /// Skips empty lines and returns all valid entries.
    ///
    /// # Returns
    ///
    /// Returns a vector of WriteEntry objects, or an error if reading fails.
    pub async fn replay(&self) -> Result<Vec<WriteEntry>> {
        let journal_path = Path::new(&self.path).join("journal.ndjson");

        if !journal_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&journal_path).map_err(|e| Db233Error::WalError(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| Db233Error::WalError(e.to_string()))?;
            if line.is_empty() {
                continue;
            }

            let entry: WriteEntry =
                serde_json::from_str(&line).map_err(|e| Db233Error::WalError(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Replays entries from the pending file.
    ///
    /// Reads the pending file line by line, deserializing each JSON entry.
    /// This is used to recover writes that were being recorded when a crash occurred.
    ///
    /// # Returns
    ///
    /// Returns a vector of WriteEntry objects, or an error if reading fails.
    pub async fn replay_pending(&self) -> Result<Vec<WriteEntry>> {
        let pending_path = Path::new(&self.pending_file);

        if !pending_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(pending_path).map_err(|e| Db233Error::WalError(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| Db233Error::WalError(e.to_string()))?;
            if line.is_empty() {
                continue;
            }

            let entry: WriteEntry =
                serde_json::from_str(&line).map_err(|e| Db233Error::WalError(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Clears the pending file.
    ///
    /// Removes the pending file if it exists. Used after successfully replaying
    /// pending entries to prevent duplicate processing.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success, or if the file doesn't exist.
    pub async fn clear_pending(&self) -> Result<()> {
        match fs::remove_file(&self.pending_file) {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(Db233Error::WalError(e.to_string()))
                }
            }
        }
    }
}
