pub mod db_errors {

    #[derive(Debug)]
    pub enum DbError {
        InvalidCommand(&'static str),
        KeyNotFound(String),
    }
}
