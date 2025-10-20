pub mod json_engine {

    use std::fs;

    use serde::{Deserialize, Serialize};

    use crate::db::storage_engine::engine::engine::Engine;
    use crate::common::db_errors::db_errors::DbError;

    #[derive(Serialize, Deserialize)]
    pub struct JsonEngine {
        pub file_path: String
    }

    impl Engine for JsonEngine {
        fn save(
            &self,
            map: std::collections::HashMap<String, String>,
        ) -> Result<(),DbError> {
            let json = serde_json::to_string(&map).map_err(|e| DbError::SaveFailed(e.to_string()))?;

            fs::write(&self.file_path, json).map_err(|e|DbError::SaveFailed(e.to_string()))

        }

        fn load(
            &self,
        ) -> Result<
            std::collections::HashMap<String, String>,
            crate::common::db_errors::db_errors::DbError,
        > {
            todo!()
        }
    }
}
