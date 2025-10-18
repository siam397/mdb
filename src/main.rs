pub mod common;
pub mod db;
use std::{collections::HashMap, io};

use crate::common::command_type::command_type::CommandType;
use crate::db::command::command::{handle_delete, handle_get, handle_set};

fn main() {
    let mut map: HashMap<String, String> = HashMap::new();

    println!("Welcome to minidb");

    loop {
        let mut user_instruction = String::new();

        io::stdin()
            .read_line(&mut user_instruction)
            .expect("Failed to read instruction");

        let splitted_instructions: Vec<&str> = user_instruction.trim().split(' ').collect();

        let instruction_type = match CommandType::from_str(splitted_instructions[0]){
            Some(val)=>val,
            None=>{
                println!("Invalid command");
                continue;
            }
        };

        match instruction_type {
            CommandType::Set => match handle_set(&splitted_instructions, &mut map) {
                Ok(_) => {
                    println!("Inserted Key {}", splitted_instructions[1])
                }
                Err(err) => {
                    eprintln!("Err: {:?}", err);
                }
            },
            CommandType::GetKeys => {
                println!("{:?}", map.keys());
            }
            CommandType::Get => match handle_get(&splitted_instructions, &map) {
                Ok(val) => {
                    println!("{:?}", val)
                }
                Err(err) => {
                    eprintln!("Err: {:?}", err);
                    continue;
                }
            },
            CommandType::Delete => {
                if let Err(err) = handle_delete(&splitted_instructions, &mut map) {
                    eprintln!("Err: {:?}", err);
                    continue;
                }
            }
        }
    }
}
