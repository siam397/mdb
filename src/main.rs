pub mod common;
pub mod db;
use std::{collections::HashMap, io};

use crate::db::command::command::{handle_delete, handle_get, handle_set};
// use db::;

fn main() {
    let mut map: HashMap<String, String> = HashMap::new();

    println!("Welcome to minidb");

    loop {
        let mut user_instruction = String::new();

        io::stdin()
            .read_line(&mut user_instruction)
            .expect("Failed to read instruction");

        let splitted_instructions: Vec<&str> = user_instruction.trim().split(' ').collect();

        let instruction_type = splitted_instructions[0];

        match instruction_type.trim() {
            "SET" => match handle_set(&splitted_instructions, &mut map) {
                Ok(_) => {
                    println!("Inserted Key {}", splitted_instructions[1])
                }
                Err(err) => {
                    eprintln!("Err: {:?}", err);
                }
            },
            "GET_KEYS" => {
                println!("{:?}", map.keys());
            }
            "GET" => match handle_get(&splitted_instructions, &map) {
                Ok(val) => {
                    println!("{:?}", val)
                }
                Err(err) => {
                    eprintln!("Err: {:?}", err);
                    continue;
                }
            },
            "DELETE" => {
                if let Err(err) = handle_delete(&splitted_instructions, &mut map) {
                    eprintln!("Err: {:?}", err);
                    continue;
                }
            }
            _ => {
                println!("Invalid instruction")
            }
        }
    }
}
