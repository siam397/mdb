pub mod command {
    use std::collections::HashMap;

    use crate::common::db_errors::db_errors::DbError;

    pub fn handle_set(
        splitted_instruction: &[&str],
        map: &mut HashMap<String, String>,
    ) -> Result<(), DbError> {
        if splitted_instruction.len() < 3 {
            return Err(DbError::InvalidCommand(
                "Invalid SET instruction. It needs a key and value",
            ));
        }

        let k = splitted_instruction[1].to_string();
        let v = splitted_instruction[2..].join(" ");

        map.insert(k, v);

        Ok(())
    }

    pub fn handle_get<'a>(
        splitted_instructions: &[&str],
        map: &'a HashMap<String, String>,
    ) -> Result<&'a str, DbError> {
        if splitted_instructions.len() < 2 {
            return Err(DbError::InvalidCommand(
                "Invalid GET instruction. It needs the key",
            ));
        }

        let key = splitted_instructions[1];

        map.get(key)
            .map(|s| s.as_str())
            .ok_or_else(|| DbError::KeyNotFound(key.to_string()))
    }

    pub fn handle_delete(
        splitted_instruction: &[&str],
        map: &mut HashMap<String, String>,
    ) -> Result<(), DbError> {
        if splitted_instruction.len() < 2 {
            return Err(DbError::InvalidCommand(
                "Number of argument too low for delete. Need to know the key",
            ));
        }

        let key = splitted_instruction[1];

        map.remove(key);

        println!("Deleted key {}", splitted_instruction[1]);
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;

        use super::*;

        #[test]
        fn test_handle_set_and_get() {
            let mut map = HashMap::new();
            let set_cmd = vec!["SET", "username", "siam"];
            let get_cmd = vec!["GET", "username"];

            // Test SET
            let res = handle_set(&set_cmd, &mut map);
            assert!(res.is_ok());
            assert_eq!(map.get("username"), Some(&"siam".to_string()));

            // Test GET
            let res = handle_get(&get_cmd, &mut map);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), "siam");
        }

        #[test]
        fn test_handle_get_missing_key() {
            let mut map = HashMap::new();
            let get_cmd = vec!["GET", "does_not_exist"];

            let res = handle_get(&get_cmd, &mut map);
            assert!(res.is_err());
        }

        #[test]
        fn test_handle_delete() {
            let mut map = HashMap::new();
            let set_cmd = vec!["SET", "token", "abc123"];
            let del_cmd = vec!["DELETE", "token"];

            handle_set(&set_cmd, &mut map).unwrap();
            assert!(map.contains_key("token"));

            handle_delete(&del_cmd, &mut map).unwrap();
            assert!(!map.contains_key("token"));
        }

        #[test]
        fn test_handle_set_invalid_args() {
            let mut map = HashMap::new();
            let bad_cmd = vec!["SET", "only_one_arg"];

            let res = handle_set(&bad_cmd, &mut map);
            assert!(res.is_err());
        }
    }
}
