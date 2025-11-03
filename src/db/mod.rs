use std::collections::BTreeMap;

use crate::{
    common::{command_type::CommandType, db_errors::DbError},
    storage_engine::engine::Engine,
    wal::Wal,
};

pub struct Db<E: Engine> {
    pub data: BTreeMap<String, String>,
    pub engine: E,
    pub wal: Wal<E>,
}

impl<E: Engine> Db<E> {
    pub fn new(engine: E, wal: Wal<E>) -> Result<Self, DbError> {
        let data = engine.load()?;
        Ok(Db { data, engine, wal })
    }

    pub fn handle_set(&mut self, splitted_instruction: &[&str]) -> Result<(), DbError> {
        if splitted_instruction.len() < 3 {
            return Err(DbError::InvalidCommand(
                "Invalid SET instruction. It needs a key and value",
            ));
        }

        let k = splitted_instruction[1].to_string();
        let v = splitted_instruction[2..].join(" ");

        self.wal.store_wal(CommandType::Set.as_str(), &k, &v)?;

        self.data.insert(k, v);

        Ok(())
    }

    pub fn handle_get(&self, splitted_instructions: &[&str]) -> Result<String, DbError> {
        if splitted_instructions.len() < 2 {
            return Err(DbError::InvalidCommand(
                "Invalid GET instruction. It needs the key",
            ));
        }

        let key = splitted_instructions[1];

        let potential_res = self.data.get(key);

        match potential_res {
            Some(x) => Ok(x.to_string()),
            None => match self.engine.get_value(key.to_string()) {
                Ok(val) => Ok(val),
                Err(e) => Err(e),
            },
        }
    }

    pub fn handle_delete(&mut self, splitted_instruction: &[&str]) -> Result<(), DbError> {
        if splitted_instruction.len() < 2 {
            return Err(DbError::InvalidCommand(
                "Number of argument too low for delete. Need to know the key",
            ));
        }

        let key = splitted_instruction[1];
        self.wal.store_wal(
            CommandType::Delete.as_str(),
            &key.to_string(),
            &String::new(),
        )?;

        self.data.remove(key);

        println!("Deleted key {}", splitted_instruction[1]);
        Ok(())
    }

    pub fn flush_to_persist(&self) {
        println!("flushhhhhhhh")
    }
}
