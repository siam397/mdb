use std::cmp::Ordering;
use std::time::SystemTime;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
};

use chrono::Utc;

use crate::{common::db_errors::DbError, storage_engine::engine::Engine};

pub struct SSTableEngine {
    pub file_path: String,
}

impl Engine for SSTableEngine {
    fn new(file_path: String) -> Self {
        SSTableEngine { file_path }
    }

    fn compact_sstables(&self) -> Result<(), DbError> {
        let cutoff = SystemTime::now() - std::time::Duration::from_secs(5);
        let entries = fs::read_dir(&self.file_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
        
        let mut files_to_compact: Vec<(String, SystemTime)> = vec![];
        let mut merged_data: BTreeMap<String, String> = BTreeMap::new();

        // Collect files older than 5 seconds
        for entry_result in entries {
            let entry = entry_result.map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("db") {
                let metadata = entry.metadata().map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
                let modified = metadata.modified().map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

                if modified <= cutoff {
                    if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                        files_to_compact.push((filename.to_string(), modified));
                    }
                }
            }
        }

        // Sort files by modification time (newest first) to ensure we get the latest values
        files_to_compact.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        // Read and merge data from all files
        for (filename, _) in files_to_compact.iter() {
            let full_path = format!("{}/{}", self.file_path, filename);
            let file = File::open(&full_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let reader = BufReader::new(file);

            for line_result in reader.lines() {
                let line = line_result.map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
                let parts: Vec<&str> = line.splitn(2, " ").collect();
                
                if parts.len() == 2 {
                    let key = parts[0].to_string();
                    let value = parts[1].to_string();

                    // Only add/update if:
                    // 1. Key doesn't exist in merged data yet (newer value already processed)
                    // 2. Value is not a tombstone
                    if !merged_data.contains_key(&key) && !value.contains("___________TOMBSTONE________________") {
                        merged_data.insert(key, value);
                    }
                }
            }
        }

        // Write merged data to new SSTable file
        if !merged_data.is_empty() {
            let now = Utc::now();
            let timestamp = now.timestamp();
            let new_file_path = format!("{}/compacted_{}.db", self.file_path, timestamp);
            
            let file = File::create(&new_file_path).map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            let mut writer = BufWriter::new(file);

            for (key, value) in merged_data {
                let line = format!("{} {}\n", key, value);
                writer.write_all(line.as_bytes()).map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
                writer.flush().map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            }

            // Delete old files after successful compaction
            for (filename, _) in files_to_compact {
                let file_path = format!("{}/{}", self.file_path, filename);
                fs::remove_file(file_path).map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn save_all(&self, map: &std::collections::BTreeMap<String, String>) -> Result<(), DbError> {
        let now = Utc::now();
        let timestamp = now.timestamp();

        let full_path = format!("{}/{}.db", self.file_path, timestamp);

        let file =
            File::create(&full_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

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
        let map: BTreeMap<String, String> = BTreeMap::new();
        Ok(map)
    }

    fn get_value(&self, k: String) -> Result<String, DbError> {
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
                    std::cmp::Ordering::Equal => {
                        if val
                            .to_string()
                            .contains("___________TOMBSTONE________________")
                        {
                            return Err(DbError::KeyNotFound(format!(
                                "Key not found for key: {}",
                                k
                            )));
                        }
                        return Ok(val.to_string());
                    }
                    std::cmp::Ordering::Greater => break,
                };
            }
        }

        Err(DbError::KeyNotFound(format!(
            "Key not found for key: {}",
            k
        )))
    }
}

pub fn get_sstable_files(file_dir: &str) -> Result<Vec<String>, DbError> {
    let entries = fs::read_dir(file_dir).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

    let mut files_with_time: Vec<(String, SystemTime)> = vec![];

    for entry_result in entries {
        let entry = entry_result.map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
        let path = entry.path();

        if path.is_file()
            && let Some(filename) = path.file_name().and_then(|f| f.to_str())
            && filename.ends_with(".db")
        {
            let metadata = entry
                .metadata()
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let modified = metadata
                .modified()
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

            files_with_time.push((filename.to_string(), modified));
        }
    }

    files_with_time.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    let files: Vec<String> = files_with_time.into_iter().map(|(name, _)| name).collect();

    Ok(files)
}
