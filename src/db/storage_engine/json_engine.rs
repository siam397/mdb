use std::collections::HashMap;
use std::fs;

use serde::{Deserialize, Serialize};

use crate::common::db_errors::DbError;
use crate::db::storage_engine::engine::Engine;

#[derive(Serialize, Deserialize)]
pub struct JsonEngine {
    pub file_path: String,
}

impl Engine for JsonEngine {
    fn save(&self, map: &HashMap<String, String>) -> Result<(), DbError> {
        let json = serde_json::to_string(&map).map_err(|e| DbError::SaveFailed(e.to_string()))?;

        fs::write(&self.file_path, json).map_err(|e| DbError::SaveFailed(e.to_string()))
    }

    fn load(&self) -> Result<HashMap<String, String>, DbError> {
        if !std::path::Path::new(&self.file_path).exists() {
            return Ok(HashMap::new());
        }

        let content =
            fs::read_to_string(&self.file_path).map_err(|e| DbError::LoadFailed(e.to_string()))?;

        let data =
            serde_json::from_str(&content).map_err(|e| DbError::LoadFailed(e.to_string()))?;

        Ok(data)
    }

    fn new(file_path: String) -> Self {
        JsonEngine { file_path }
    }
}
