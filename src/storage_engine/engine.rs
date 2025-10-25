use std::collections::BTreeMap;

use crate::common::db_errors::DbError;

pub trait Engine {
    fn new(file_path: String) -> Self;
    fn save_all(&self, map: &BTreeMap<String, String>) -> Result<(), DbError>;
    fn save(&self, k: String, v: String) -> Result<(), DbError>;
    fn load(&self) -> Result<BTreeMap<String, String>, DbError>;
    fn get_value(&self, k: String) -> Result<Option<String>, DbError>;
}
