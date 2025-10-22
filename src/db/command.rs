use std::collections::HashMap;

use crate::{common::db_errors::DbError, db::storage_engine::engine::Engine};

pub struct Db<E: Engine> {
    pub data: HashMap<String, String>,
    pub engine: E,
}

impl<E: Engine> Db<E> {
    pub fn new(engine: E) -> Result<Self, DbError> {
        let data = engine.load()?;
        Ok(Db { data, engine })
    }

    pub fn handle_set(&mut self, splitted_instruction: &[&str]) -> Result<(), DbError> {
        if splitted_instruction.len() < 3 {
            return Err(DbError::InvalidCommand(
                "Invalid SET instruction. It needs a key and value",
            ));
        }

        let k = splitted_instruction[1].to_string();
        let v = splitted_instruction[2..].join(" ");

        self.data.insert(k, v);

        self.engine.save_all(&self.data)?;

        Ok(())
    }

    pub fn handle_get(&self, splitted_instructions: &[&str]) -> Result<&str, DbError> {
        if splitted_instructions.len() < 2 {
            return Err(DbError::InvalidCommand(
                "Invalid GET instruction. It needs the key",
            ));
        }

        let key = splitted_instructions[1];

        self.data
            .get(key)
            .map(|s| s.as_str())
            .ok_or_else(|| DbError::KeyNotFound(key.to_string()))
    }

    pub fn handle_delete(&mut self, splitted_instruction: &[&str]) -> Result<(), DbError> {
        if splitted_instruction.len() < 2 {
            return Err(DbError::InvalidCommand(
                "Number of argument too low for delete. Need to know the key",
            ));
        }

        let key = splitted_instruction[1];

        self.data.remove(key);

        println!("Deleted key {}", splitted_instruction[1]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // A simple in-memory engine used for testing
    struct MockEngine {
        pub storage: HashMap<String, String>,
    }

    impl MockEngine {
        fn new() -> Self {
            MockEngine {
                storage: HashMap::new(),
            }
        }
    }

    impl Engine for MockEngine {
        fn new(file_path: String) -> Self {
            // file_path is unused for the mock
            let _ = file_path;
            MockEngine::new()
        }

        fn save_all(&self, map: &HashMap<String, String>) -> Result<(), DbError> {
            // In the mock, we don't persist to disk; just ensure it's serializable
            let _ = serde_json::to_string(map).map_err(|e| DbError::SaveFailed(e.to_string()))?;
            Ok(())
        }

        fn load(&self) -> Result<HashMap<String, String>, DbError> {
            // Return a copy of the internal storage
            Ok(self.storage.clone())
        }
        
        fn save(&self, k: String,v: String) -> Result<(), DbError> {
            todo!()
        }
    }

    #[test]
    fn handle_set_inserts_and_saves() {
        let engine = MockEngine::new();
        let mut db = Db::new(engine).expect("Failed to create db");

        let input = vec!["SET", "foo", "bar"];
        db.handle_set(&input).expect("handle_set failed");

        assert_eq!(db.data.get("foo"), Some(&"bar".to_string()));
    }

    #[test]
    fn handle_get_returns_value_or_error() {
        let mut engine = MockEngine::new();
        engine.storage.insert("k1".to_string(), "v1".to_string());

        let db = Db::new(engine).expect("Failed to create db");

        let res = db.handle_get(&["GET", "k1"]).expect("get failed");
        assert_eq!(res, "v1");

        let err = db.handle_get(&["GET", "missing"]).unwrap_err();
        match err {
            DbError::KeyNotFound(_) => {}
            _ => panic!("Expected KeyNotFound error"),
        }
    }

    #[test]
    fn handle_delete_removes_key() {
        let mut engine = MockEngine::new();
        engine.storage.insert("to_delete".to_string(), "v".to_string());

        let mut db = Db::new(engine).expect("Failed to create db");

        // ensure present
        assert!(db.data.get("to_delete").is_some());

        db.handle_delete(&["DEL", "to_delete"]).expect("delete failed");

        assert!(db.data.get("to_delete").is_none());
    }
}
