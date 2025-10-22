
use std::collections::HashMap;

use crate::{common::db_errors::DbError, storage_engine::engine::Engine, };

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
