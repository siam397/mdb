use std::{
    collections::BTreeMap, fs::{self, File}, io::{BufRead, BufReader, BufWriter, Write}
};

use chrono::Local;

use crate::{common::db_errors::DbError, storage_engine::engine::Engine};

pub struct SSTableEngine {
    pub file_path: String,
}

impl Engine for SSTableEngine {
    fn new(file_path: String) -> Self {
        SSTableEngine { file_path }
    }

    fn save_all(&self, map: &std::collections::BTreeMap<String, String>) -> Result<(), DbError> {
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:00").to_string();

        let full_path = format!("{}/{}.db", self.file_path, timestamp);

        let file = File::create(&full_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

        let mut writer = BufWriter::new(file);

        for (key, val) in map {
            let line = format!("{} {}\n", key, val);
            writer
                .write_all(line.as_bytes())
                .map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            writer
                .flush()
                .map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
        }

        Ok(())
    }

    fn save(&self, _k: String, _v: String) -> Result<(), DbError> {
        Ok(())
    }

    fn load(&self) -> Result<BTreeMap<String, String>, DbError> {
        let map:BTreeMap<String, String> = BTreeMap::new();
        Ok(map)
    }

    fn get_value(&self, k: String) -> Result<Option<String>, DbError> {
        let files = get_sstable_files(&self.file_path)?;

        for file in files {
            let full_path = format!("{}/{}", self.file_path, file);

            let file =
                File::open(&full_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

            let reader = BufReader::new(&file);

            for line_result in reader.lines() {
                let line = line_result.map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

                let parts: Vec<&str> = line.splitn(2, " ").collect();

                let key = parts[0];
                let val = parts[1];

                match key.cmp(k.as_str()) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => return Ok(Some(val.to_string())),
                    std::cmp::Ordering::Greater => return Ok(None),
                };
            }
        }

        Ok(None)
    }
}

// Get list of all SSTable files (sorted by timestamp, newest first)
pub fn get_sstable_files(file_dir: &str) -> Result<Vec<String>, DbError> {
    let entries = fs::read_dir(file_dir).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

    let mut files: Vec<String> = vec![];

    for entry_result in entries {
        let entry = entry_result.map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

        let path = entry.path();

        if path.is_file()
            && let Some(filename) = path.file_name().and_then(|f| f.to_str())
                && filename.starts_with("sstable_") && filename.ends_with(".db") {
                    files.push(filename.to_string());
                }
    }

    // Sort by filename (timestamp embedded) - newest first
    files.sort_by(|a, b| b.cmp(a));

    Ok(files)
}
