use std::{collections::HashMap, io};

fn main() {

    let mut map:HashMap<String, String> = HashMap::new();

    println!("Welcome to minidb");

    loop{
        let mut user_instruction = String::new();
        
        io::stdin()
            .read_line(&mut user_instruction)
            .expect("Failed to read instruction");

        let splitted_instructions:Vec<&str> = user_instruction.trim().split(' ').collect();

        let instruction_type = splitted_instructions[0];

        match instruction_type.trim() {
            "SET" => {
                
                if let Err(err) = handle_set(&splitted_instructions, &mut map){
                    eprintln!("Err: {}", err);
                    continue;
                }
            },
            "GET_KEYS" =>{
                println!("{:?}",map.keys());
            }
            "GET" => {
                match handle_get(&splitted_instructions, &mut map) {
                    Ok(val)=>{
                        println!("{:?}", val)
                    },
                    Err(err)=>{
                        eprintln!("Err: {}", err);
                        continue;
                    }
                }
            },
            "DELETE" => {
                if let Err(err) = handle_delete(&splitted_instructions, &mut map) {
                    eprintln!("Err: {}", err);
                    continue;
                }
            },
            _ => {
                println!("Invalid instruction")
            }
        }

    }

}

fn handle_set(splitted_instruction:&[&str], map: &mut HashMap<String, String>) -> Result<(), String>{
    if splitted_instruction.len() < 3 {
        return Err("Invalid SET instruction. It needs a key and value".to_string())
    }

    let k = splitted_instruction[1].to_string();
    let v = splitted_instruction[2..].join(" ");

    map.insert(k, v);

    println!("Inserted Key {}",splitted_instruction[1..].join(" "));

    Ok(())
}


fn handle_get<'a>(splitted_instructions:&[&str], map: &'a HashMap<String, String>) -> Result<&'a str, String>{
    if splitted_instructions.len() < 2 {
        return Err("Invalid GET instruction. It needs the key".to_string())
    }

    let key = splitted_instructions[1..].join(" ").to_string();

    let potential_value = map.get(&key);

    match potential_value {
        Some(val) => {
            return Ok(val.as_str());
        },
        None => {
            return Err(format!("Value doesn't exist for key: {}", key));
        }
    }
}

fn handle_delete(splitted_instruction:&[&str], map: &mut HashMap<String, String>) -> Result<(), String>{
    if splitted_instruction.len() < 2 {
        return Err("Number of argument too low for delete. Need to know the key".to_string());
    }

    let key = splitted_instruction[1..].join(" ");

    map.remove(&key);
    
    println!("Deleted key {}", splitted_instruction[1..].join(" "));
    Ok(())
}


#[cfg(test)]
mod tests{
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
        assert_eq!(res.unwrap_err(), "Value doesn't exist for key: does_not_exist");
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
        assert_eq!(res.unwrap_err(), "Invalid SET instruction. It needs a key and value");
    }

}