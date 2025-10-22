use std::{fs::{File, OpenOptions}, io::{BufWriter, Read, Write}};

use crate::{common::db_errors::DbError, db::storage_engine::engine::Engine};

pub struct BufferEngine{
    pub file_path: String
}

impl Engine for BufferEngine {
    fn new(file_path: String) -> Self {
        BufferEngine { file_path }
    }

    fn save_all(&self, map: &std::collections::HashMap<String, String>) -> Result<(), DbError> {
        let file = OpenOptions::new().write(true).append(true).open(&self.file_path).map_err(|e|DbError::SaveFailed(e.to_string()))?;

        let mut writer = BufWriter::new(file);

        let json = serde_json::to_string(&map).map_err(|e| DbError::SaveFailed(e.to_string()))?;

        writer.write_all(json.as_bytes()).map_err(|e|DbError::SaveFailed(e.to_string()))?;
        writer.flush().map_err(|e|DbError::SaveFailed(e.to_string()))?;

        Ok(())

    }


    fn load(&self) -> Result<std::collections::HashMap<String, String>, crate::common::db_errors::DbError> {
        todo!()
    }
    
    fn save(&self, _k: String, _v: String) -> Result<(), DbError> {
        todo!()
    }
}