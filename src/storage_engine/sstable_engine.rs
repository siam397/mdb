use std::cmp::Ordering;
use std::time::SystemTime;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
};

use chrono::Utc;

use crate::ende::{read_key_from_binary_file, write_btree_to_binary_file};
use crate::{common::db_errors::DbError, storage_engine::engine::Engine};

pub struct SSTableEngine {
    pub file_path: String,
}

impl Engine for SSTableEngine {
    fn new(file_path: String) -> Self {
        SSTableEngine { file_path }
    }

    fn compact_sstables(&self) -> Result<(), DbError> {
        let files_to_compact = get_sstable_files(&self.file_path)?;
        let mut merged_data: BTreeMap<String, String> = BTreeMap::new();

        for filename in &files_to_compact {
            let full_path = format!("{}/{}", self.file_path, filename);
            let file = File::open(&full_path)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let metadata = file
                .metadata()
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let file_len = metadata.len();

            // read footer (to get index offset)
            let mut f = file;
            f.seek(SeekFrom::Start(file_len - 16))
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let mut buf = [0u8; 8];
            f.read_exact(&mut buf)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let index_offset = u64::from_be_bytes(buf);

            // jump to index section
            f.seek(SeekFrom::Start(index_offset))
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

            let index_end = file_len - 16;
            while f.stream_position().unwrap() < index_end {
                // read key_len
                let mut key_len_buf = [0u8; 4];
                f.read_exact(&mut key_len_buf)
                    .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
                let key_len = u32::from_be_bytes(key_len_buf) as usize;

                // read key
                let mut key_buf = vec![0u8; key_len];
                f.read_exact(&mut key_buf)
                    .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
                let key = String::from_utf8(key_buf)
                    .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

                // read offset
                let mut off_buf = [0u8; 8];
                f.read_exact(&mut off_buf)
                    .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
                let _offset = u64::from_be_bytes(off_buf);

                // get value from this SSTable
                match read_key_from_binary_file(&full_path, &key) {
                    Ok(value) => {
                        if !value.contains("___________TOMBSTONE________________") {
                            // only keep if key is not yet merged (newer SSTables come first)
                            if !merged_data.contains_key(&key) {
                                merged_data.insert(key, value);
                            }
                        }
                    }
                    Err(DbError::TombStoneFound) => {
                        // skip tombstones
                    }
                    Err(_) => {
                        // ignore corrupted key/value gracefully
                        continue;
                    }
                }
            }
        }

        // Write merged data if any
        if !merged_data.is_empty() {
            let now = Utc::now();
            let timestamp = now.timestamp();
            let new_file_path = format!("{}/compacted_{}.db", self.file_path, timestamp);

            write_btree_to_binary_file(&merged_data, &new_file_path)?;

            // Remove old SSTables
            for filename in files_to_compact {
                let file_path = format!("{}/{}", self.file_path, filename);
                fs::remove_file(file_path)
                    .map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            }
        }

        Ok(())
    }
    

    fn save_all(&self, map: &std::collections::BTreeMap<String, String>) -> Result<(), DbError> {
        let now = Utc::now();
        let timestamp = now.timestamp();

        let full_path = format!("{}/{}.db", self.file_path, timestamp);

        write_btree_to_binary_file(map, &full_path)
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

            match read_key_from_binary_file(&full_path, &k) {
                Ok(val) => return Ok(val),
                Err(e) => {
                    if matches!(e, DbError::TombStoneFound) {
                        break;
                    }
                    if matches!(e, DbError::KeyNotInFile) {
                        continue;
                    }
                    println!("{:?}", e);
                    continue;
                }
            };
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
