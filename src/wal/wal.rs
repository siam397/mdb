use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    time::{Duration, SystemTime},
};

use chrono::Local;

use crate::common::db_errors::DbError;

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
            .map_err(|e| {
                DbError::WalStoreFailed(format!(
                    "Failed to read from wal log. Err: {}",
                    e.to_string()
                ))
            })?;

        Ok(())
    }

    pub fn play_wal_to_store(&self) -> Result<(), DbError> {
        let cutoff = SystemTime::now() - Duration::from_secs(60);

        let entries =
            fs::read_dir(&self.file_dir).map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

        for potential_entry in entries {
            let entry = potential_entry.map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

            let path = entry.path();

            if path.is_file() {
                let metadata =
                    fs::metadata(&path).map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

                let file_created_at = metadata
                    .created()
                    .map_err(|e| DbError::WalStoreFailed(e.to_string()))?;

                if file_created_at < cutoff {

                }
            }
        }

        Ok(())
    }
    

}
