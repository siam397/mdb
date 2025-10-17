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
                
                println!("Inserted Key {}",splitted_instructions[1])
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
            _ => {
                println!("Invalid instruction")
            }
        }

    }

}

fn handle_set(splitted_instruction:&Vec<&str>, map: &mut HashMap<String, String>) -> Result<(), String>{
    if splitted_instruction.len() < 3 {
        return Err("Invalid SET instruction. It needs a key and value".to_string())
    }

    let k = splitted_instruction[1].to_string();
    let v = splitted_instruction[2..].join(" ");

    map.insert(k, v);

    Ok(())
}

fn handle_get(splitted_instructions:&Vec<&str>, map: &mut HashMap<String, String>) -> Result<String, String>{
    if splitted_instructions.len() < 2 {
        return Err("Invalid GET instruction. It needs the key".to_string())
    }

    let key = splitted_instructions[1..].join(" ").to_string();

    let potential_value = map.get(&key);

    match potential_value {
        Some(val) => {
            return Ok(val.to_string());
        },
        None => {
            return Err(format!("Value doesn't exist for key: {}", key))
        }
    }
}