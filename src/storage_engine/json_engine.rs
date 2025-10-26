// deprecated
use std::collections::BTreeMap;
use std::fs;

use serde::{Deserialize, Serialize};

use crate::{common::db_errors::DbError, storage_engine::engine::Engine};

#[derive(Serialize, Deserialize)]
pub struct JsonEngine {
    pub file_path: String,
}

impl Engine for JsonEngine {
    fn save_all(&self, map: &BTreeMap<String, String>) -> Result<(), DbError> {
        let json = serde_json::to_string(&map).map_err(|e| DbError::SaveFailed(e.to_string()))?;

        fs::write(&self.file_path, json).map_err(|e| DbError::SaveFailed(e.to_string()))
    }

    fn load(&self) -> Result<BTreeMap<String, String>, DbError> {
        if !std::path::Path::new(&self.file_path).exists() {
            return Ok(BTreeMap::new());
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

    fn save(&self, _k: String, _v: String) -> Result<(), DbError> {
        todo!()
    }

    fn get_value(&self, _k: String) -> Result<Option<String>, DbError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("minidb_test_{}.json", now));
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn save_and_load_roundtrip() {
        let path = unique_temp_path();

        // ensure a clean start
        let _ = fs::remove_file(&path);

        let engine = JsonEngine::new(path.clone());

        let mut map = BTreeMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());

        engine.save_all(&map).expect("save failed");

        let engine2 = JsonEngine::new(path.clone());
        let loaded = engine2.load().expect("load failed");

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get("key1"), Some(&"value1".to_string()));

        // cleanup
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_nonexistent_returns_empty() {
        let path = unique_temp_path();

        // ensure file doesn't exist
        let _ = fs::remove_file(&path);

        let engine = JsonEngine::new(path.clone());
        let loaded = engine.load().expect("load failed");
        assert!(loaded.is_empty());
    }
}
