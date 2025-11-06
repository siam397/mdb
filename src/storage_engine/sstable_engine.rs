use std::cmp::Ordering;
use std::time::SystemTime;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write, Seek, Read, SeekFrom},
};

use chrono::Utc;

use crate::{common::db_errors::DbError, storage_engine::engine::Engine};

const MAGIC_HEADER: &[u8; 8] = b"MINIDBSS";
const MAGIC_FOOTER: &[u8; 8] = b"MINIDIDX";
const VERSION: u8 = 1;

// Write a u32 in big-endian format
pub fn write_u32_be(writer: &mut impl Write, value: u32) -> std::io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

// Write a u64 in big-endian format
pub fn write_u64_be(writer: &mut impl Write, value: u64) -> std::io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

/// Write a BTreeMap to a binary SSTable file
/// File format:
/// - Header (16 bytes):
///   - Magic (8 bytes): "MINIDBSS"
///   - Version (1 byte)
///   - Reserved (7 bytes)
/// - Data section:
///   For each record:
///   - key_len (u32 BE)
///   - key (bytes)
///   - tombstone (u8): 0=present, 1=deleted
///   - value_len (u32 BE) - only if not tombstone
///   - value (bytes) - only if not tombstone
/// - Index section:
///   For each key:
///   - key_len (u32 BE)
///   - key (bytes)
///   - offset (u64 BE)
/// - Footer:
///   - index_offset (u64 BE)
///   - Magic (8 bytes): "MINIDIDX"
pub fn write_btree_to_binary_file(map: &BTreeMap<String, String>, file_path: &str) -> Result<(), DbError> {
    let mut index_entries: Vec<(String, u64)> = Vec::new();
    
    // Open file with BufWriter for efficient writing
    let file = File::create(file_path)
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to create file: {}", e)))?;
    let mut writer = BufWriter::new(file);
    
    // Write header
    writer.write_all(MAGIC_HEADER)
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write header magic: {}", e)))?;
    writer.write_all(&[VERSION])
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write version: {}", e)))?;
    writer.write_all(&[0; 7]) // Reserved bytes
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write reserved bytes: {}", e)))?;

    // Write data section and collect index entries
    for (key, value) in map {
        // Record the offset for this key
        let record_offset = writer.stream_position()
            .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to get position: {}", e)))?;
        index_entries.push((key.clone(), record_offset));

        // Write key length and key
        write_u32_be(&mut writer, key.len() as u32)
            .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write key length: {}", e)))?;
        writer.write_all(key.as_bytes())
            .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write key: {}", e)))?;

        // Check if value is a tombstone
        let is_tombstone = value.contains("___________TOMBSTONE________________");
        if is_tombstone {
            writer.write_all(&[1]) // Tombstone flag
                .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write tombstone flag: {}", e)))?;
        } else {
            writer.write_all(&[0]) // Not tombstone
                .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write tombstone flag: {}", e)))?;
            write_u32_be(&mut writer, value.len() as u32)
                .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write value length: {}", e)))?;
            writer.write_all(value.as_bytes())
                .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write value: {}", e)))?;
        }
    }

    // Write index section
    let index_offset = writer.stream_position()
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to get index position: {}", e)))?;
    
    // Write each index entry
    for (key, offset) in index_entries {
        write_u32_be(&mut writer, key.len() as u32)
            .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write index key length: {}", e)))?;
        writer.write_all(key.as_bytes())
            .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write index key: {}", e)))?;
        write_u64_be(&mut writer, offset)
            .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write index offset: {}", e)))?;
    }

    // Write footer
    write_u64_be(&mut writer, index_offset)
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write index offset: {}", e)))?;
    writer.write_all(MAGIC_FOOTER)
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to write footer magic: {}", e)))?;

    // Ensure all data is written to disk
    writer.flush()
        .map_err(|e| DbError::SSTableWriteFailed(format!("Failed to flush writer: {}", e)))?;

    Ok(())
}

/// Read a single key from a binary SSTable file and return its value as a UTF-8 string.
/// If the key is marked as tombstone or not found, returns `DbError::KeyNotFound`.
pub fn read_key_from_binary_file(file_path: &str, search_key: &str) -> Result<String, DbError> {
    let mut file = File::open(file_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

    let metadata = file
        .metadata()
        .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
    let file_len = metadata.len();

    // Footer is 8 (u64 index_offset) + 8 (magic)
    if file_len < 16 {
        return Err(DbError::SSTableReadFailed("sstable file too small".to_string()));
    }

    // Seek to footer
    file.seek(SeekFrom::Start(file_len - 16))
        .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

    let mut buf8 = [0u8; 8];
    file.read_exact(&mut buf8)
        .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
    let index_offset = u64::from_be_bytes(buf8);

    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)
        .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
    if &magic != MAGIC_FOOTER {
        return Err(DbError::SSTableReadFailed("invalid sstable footer magic".to_string()));
    }

    // Seek to index and scan entries to find the key
    file.seek(SeekFrom::Start(index_offset))
        .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

    // The index runs until footer (file_len - 16)
    let index_end = file_len - 16;

    while file.stream_position().map_err(|e| DbError::SSTableReadFailed(e.to_string()))? < index_end {
        // read key_len
        let mut key_len_buf = [0u8; 4];
        file.read_exact(&mut key_len_buf)
            .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
        let key_len = u32::from_be_bytes(key_len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        file.read_exact(&mut key_buf)
            .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
        let key_str = String::from_utf8(key_buf.clone())
            .map_err(|e| DbError::SSTableReadFailed(format!("invalid utf8 in index key: {}", e)))?;

        // read offset
        let mut off_buf = [0u8; 8];
        file.read_exact(&mut off_buf)
            .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
        let record_offset = u64::from_be_bytes(off_buf);

        if key_str == search_key {
            // Found index entry. Seek to record and read it.
            file.seek(SeekFrom::Start(record_offset))
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

            // read record key_len and key (we can skip validating but do it to advance cursor)
            let mut rec_key_len_buf = [0u8; 4];
            file.read_exact(&mut rec_key_len_buf)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let rec_key_len = u32::from_be_bytes(rec_key_len_buf) as usize;
            let mut rec_key_buf = vec![0u8; rec_key_len];
            file.read_exact(&mut rec_key_buf)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

            // tombstone flag
            let mut tomb_buf = [0u8; 1];
            file.read_exact(&mut tomb_buf)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            if tomb_buf[0] == 1 {
                return Err(DbError::KeyNotFound(format!("Key not found for key: {}", search_key)));
            }

            // read value length and value
            let mut val_len_buf = [0u8; 4];
            file.read_exact(&mut val_len_buf)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
            let val_len = u32::from_be_bytes(val_len_buf) as usize;
            let mut val_buf = vec![0u8; val_len];
            file.read_exact(&mut val_buf)
                .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

            let value = String::from_utf8(val_buf)
                .map_err(|e| DbError::SSTableReadFailed(format!("invalid utf8 in value: {}", e)))?;
            return Ok(value);
        }
    }

    Err(DbError::KeyNotFound(format!("Key not found for key: {}", search_key)))
}

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
        // Read and merge data from all files
        for filename in files_to_compact.iter() {
            let full_path = format!("{}/{}", self.file_path, filename);
            let file =
                File::open(&full_path).map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;
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
                    if !merged_data.contains_key(&key)
                        && !value.contains("___________TOMBSTONE________________")
                    {
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

            let file = File::create(&new_file_path)
                .map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            let mut writer = BufWriter::new(file);

            for (key, value) in merged_data {
                let line = format!("{} {}\n", key, value);
                writer
                    .write_all(line.as_bytes())
                    .map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
                writer
                    .flush()
                    .map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            }

            // Delete old files after successful compaction
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
