use crate::error::{Db233Error, Result};
use mysql_async::Value;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteEntry {
    pub timestamp: i64,
    pub sql: String,
    pub params: Vec<ValueSerde>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueSerde {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    Bytes(Vec<u8>),
}

impl From<Value> for ValueSerde {
    fn from(value: Value) -> Self {
        match value {
            Value::NULL => ValueSerde::Null,
            Value::Int(i) => ValueSerde::Int(i),
            Value::UInt(u) => ValueSerde::UInt(u),
            Value::Float(f) => ValueSerde::Float(f.into()),
            Value::Double(f) => ValueSerde::Float(f),
            Value::Date(y, m, d, hh, mm, ss, _) => {
                ValueSerde::Bytes(format!("{}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss).into_bytes())
            }
            Value::Time(_neg, _days, hh, mm, ss, _) => {
                ValueSerde::Bytes(format!("{:02}:{:02}:{:02}", hh, mm, ss).into_bytes())
            }
            Value::Bytes(b) => ValueSerde::Bytes(b),
        }
    }
}

impl From<ValueSerde> for Value {
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

pub struct LocalWriteJournal {
    path: String,
    writer: Arc<Mutex<BufWriter<File>>>,
    running: AtomicBool,
    pending_file: String,
}

impl LocalWriteJournal {
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

    pub async fn record_write(&self, sql: &str, params: &[Value]) -> Result<()> {
        if !self.running.load(Ordering::Acquire) {
            return Ok(());
        }

        let entry = WriteEntry {
            timestamp: chrono::Utc::now().timestamp(),
            sql: sql.to_string(),
            params: params.iter().map(|p| (*p).clone().into()).collect(),
        };

        let json_str = serde_json::to_string(&entry)
            .map_err(|e| Db233Error::WalError(e.to_string()))?;

        let mut writer = self.writer.lock().await;
        writeln!(writer, "{}", json_str)
            .map_err(|e| Db233Error::WalError(e.to_string()))?;
        writer.flush()
            .map_err(|e| Db233Error::WalError(e.to_string()))?;

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::Release);

        let mut writer = self.writer.lock().await;
        writer.flush()
            .map_err(|e| Db233Error::WalError(e.to_string()))?;

        Ok(())
    }

    pub async fn replay(&self) -> Result<Vec<WriteEntry>> {
        let journal_path = Path::new(&self.path).join("journal.ndjson");

        if !journal_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&journal_path)
            .map_err(|e| Db233Error::WalError(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| Db233Error::WalError(e.to_string()))?;
            if line.is_empty() {
                continue;
            }

            let entry: WriteEntry = serde_json::from_str(&line)
                .map_err(|e| Db233Error::WalError(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    pub async fn replay_pending(&self) -> Result<Vec<WriteEntry>> {
        let pending_path = Path::new(&self.pending_file);

        if !pending_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&pending_path)
            .map_err(|e| Db233Error::WalError(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| Db233Error::WalError(e.to_string()))?;
            if line.is_empty() {
                continue;
            }

            let entry: WriteEntry = serde_json::from_str(&line)
                .map_err(|e| Db233Error::WalError(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

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