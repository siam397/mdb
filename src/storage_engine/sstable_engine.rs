use std::{fs::File, io::{BufWriter, Write}};

use chrono::Local;

use crate::{common::db_errors::DbError, storage_engine::engine::Engine};

pub struct SSTableEngine {
    pub file_path: String,
}

impl Engine for SSTableEngine {
    fn new(file_path: String) -> Self {
        SSTableEngine { file_path }
    }

    fn save_all(
        &self,
        map: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), crate::common::db_errors::DbError> {

                let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:00").to_string();

        let full_path = format!("{}/{}", self.file_path,timestamp);

                let file = File::open(&full_path)
            .map_err(|e| DbError::SSTableReadFailed(e.to_string()))?;

        let mut writer = BufWriter::new(file);

        for (key, val) in map {
            let line = format!("{} {}\n",key,val);
            writer.write_all(line.as_bytes()).map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
            writer.flush().map_err(|e| DbError::SSTableWriteFailed(e.to_string()))?;
        }

        Ok(())
    }

    fn save(&self, _k: String, _v: String) -> Result<(), crate::common::db_errors::DbError> {
        todo!()
    }

    fn load(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>, crate::common::db_errors::DbError> {
        todo!()
    }
}
