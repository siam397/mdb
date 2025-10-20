pub mod command_type {
    pub enum CommandType {
        Set,
        Get,
        GetKeys,
        Delete,
    }

    impl CommandType {
        pub fn as_str(&self) -> &'static str {
            match self {
                CommandType::Set => "SET",
                CommandType::Get => "GET",
                CommandType::GetKeys => "GET_KEYS",
                CommandType::Delete => "DELETE",
            }
        }

        pub fn from_str(s: &str) -> Option<Self> {
            match s.trim().to_uppercase().as_str() {
                "SET" => Some(CommandType::Set),
                "GET" => Some(CommandType::Get),
                "GET_KEYS" => Some(CommandType::GetKeys),
                "DELETE" => Some(CommandType::Delete),
                _ => None,
            }
        }
    }
}
