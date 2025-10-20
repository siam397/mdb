use std::collections::HashMap;

use crate::common::db_errors::DbError;

pub trait Engine {
    fn new(file_path: String) -> Self;
    fn save(&self, map: &HashMap<String, String>) -> Result<(), DbError>;
    fn load(&self) -> Result<HashMap<String, String>, DbError>;
}
