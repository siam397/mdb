pub mod engine {
    use std::collections::HashMap;

    use crate::common::db_errors::db_errors::DbError;

    pub trait Engine {
        fn save(&self, map: HashMap<String, String>) -> Result<(),DbError>;
        fn load(&self) -> Result<HashMap<String, String>, DbError>;
    }
}
