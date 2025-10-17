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
            "GET" =>{
                if splitted_instructions.len() != 2 {
                    println!("Invalid Get instruction. It needs the key");
                    continue;
                }

                let fetched_key = map.get(splitted_instructions[1]);
                match fetched_key {
                    Some(val)=>{
                        println!("{}", val);
                    },
                    None => {
                        println!("Value not found");
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
