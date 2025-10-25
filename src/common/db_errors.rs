#[derive(Debug)]
pub enum DbError {
    InvalidCommand(&'static str),
    KeyNotFound(String),
    SaveFailed(String),
    LoadFailed(String),
    WalStoreFailed(String),
    SSTableReadFailed(String),
    SSTableWriteFailed(String),
}
