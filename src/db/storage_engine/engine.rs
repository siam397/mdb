use std::collections::HashMap;

use crate::common::db_errors::DbError;

pub trait Engine {
    fn new(file_path: String) -> Self;
    fn save_all(&self, map: &HashMap<String, String>) -> Result<(), DbError>;
    fn save(&self, k: String,v: String) -> Result<(), DbError>;
    fn load(&self) -> Result<HashMap<String, String>, DbError>;
}
