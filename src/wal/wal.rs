use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    time::{Duration, SystemTime},
};

use chrono::Local;

use crate::common::{command_type::CommandType, db_errors::DbError};

pub struct Wal {
    pub file_dir: String,
}

impl Wal {
    pub fn new(file_path: String) -> Self {
        Wal {
            file_dir: file_path,
        }
    }

    pub fn store_wal(
        &self,
        instruction: &str,
        key: &String,
        value: &String,
    ) -> Result<(), DbError> {
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:00").to_string();

        let filename = format!("wal_{}.log", timestamp);
        let full_file_path = format!("{}/{}", self.file_dir, filename);
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&full_file_path)
            .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

        let content = format!("{} {} {}\n", instruction, key, value);

        let mut writer = BufWriter::new(&file);

        writer
            .write_all(content.as_bytes())
            .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;
        file.sync_all()
            .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;
        Ok(())
    }

    pub fn wal_to_store(&self) -> Result<(), DbError> {
        let _file = OpenOptions::new()
            .read(true)
            .open(&self.file_dir)
            .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

        Ok(())
    }

    pub fn play_wal_to_store(&self) -> Result<(), DbError> {
        let mut map: BTreeMap<String, String> = BTreeMap::new();

        let files = self.get_wal_files_available_for_snapshot()?;

        for file in files {
            let file = File::open(&file).map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

            let reader = BufReader::new(&file);

            for instruction_result in reader.lines() {
                let instruction = instruction_result.map_err(|e| {
                    DbError::WalStoreFailed(format!("failed to read lines. ERR {}", e))
                })?;
                self.store_wals_to_map(instruction.as_str(), &mut map);
            }
        }

        Ok(())
    }
    pub fn get_wal_files_available_for_snapshot(&self) -> Result<Vec<String>, DbError> {
        let cutoff = SystemTime::now() - Duration::from_secs(60);
        let entries =
            fs::read_dir(&self.file_dir).map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

        let mut files: Vec<String> = vec![];

        for potential_entry in entries {
            let entry = potential_entry.map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

            let path = entry.path();

            if path.is_file() {
                let metadata =
                    fs::metadata(&path).map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

                let filename = path
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or("no_file")
                    .to_string();

                let file_created_at = metadata
                    .created()
                    .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

                if file_created_at < cutoff {
                    files.push(format!("wal/{}", filename));
                    // let split_inst:Vec<&str> = instruction.trim().split(" ").collect();
                }
            }
        }
        Ok(files)
    }

    pub fn store_wals_to_map(&self, instruction: &str, map: &mut BTreeMap<String, String>) {
        let split_instruction: Vec<&str> = instruction.split(" ").collect();

        if split_instruction.len() < 3 {
            return;
        }

        let instruction_type =
            CommandType::command_type_from_str(split_instruction[0]).unwrap_or(CommandType::Get);

        let key = split_instruction[1];

        let val = split_instruction[2..].join(" ");

        match instruction_type {
            CommandType::Set => map.insert(key.to_string(), val),
            CommandType::Delete => map.remove(key),
            _ => {
                todo!()
            }
        };
    }
}
