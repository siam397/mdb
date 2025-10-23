use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
};

use crate::common::db_errors::DbError;

pub struct Wal {
    pub file_path: String,
}

impl Wal {
    pub fn new(file_path: String) -> Self {
        Wal { file_path }
    }

    pub fn store_wal(
        &self,
        instruction: &str,
        key: &String,
        value: &String,
    ) -> Result<(), DbError> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.file_path)
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
        let file = OpenOptions::new()
            .read(true)
            .open(&self.file_path)
            .map_err(|e| {
                DbError::WalStoreFailed(format!(
                    "Failed to read from wal log. Err: {}",
                    e.to_string()
                ))
            })?;
        
        

        Ok(())
    }
}
