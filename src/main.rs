pub mod common;
pub mod db;
pub mod flusher;
pub mod storage_engine;
pub mod wal;
use std::{io, sync::Arc};

use crate::{
    common::command_type::CommandType, db::Db, flusher::Flusher, storage_engine::{engine::Engine, sstable_engine::SSTableEngine}, wal::Wal
};

#[tokio::main]
async fn main() {
    println!("Welcome to minidb");

    let flusher_wal = Wal::new(
        String::from("wal"),
        SSTableEngine::new(String::from("data")),
    );

    let flusher = Flusher::new(90, Arc::new(flusher_wal));
    flusher.start();

    let sstable_engine = SSTableEngine::new(String::from("data"));

    let wal = Wal::new(
        String::from("wal"),
        SSTableEngine::new(String::from("data")),
    );

    let mut db = Db::new(sstable_engine, wal).expect("Failed to load db");

    loop {
        let mut user_instruction = String::new();

        io::stdin()
            .read_line(&mut user_instruction)
            .expect("Failed to read instruction");

        let splitted_instructions: Vec<&str> = user_instruction.trim().split(' ').collect();

        let instruction_type = match CommandType::command_type_from_str(splitted_instructions[0]) {
            Some(val) => val,
            None => {
                println!("Invalid command");
                continue;
            }
        };

        match instruction_type {
            CommandType::Set => match db.handle_set(&splitted_instructions) {
                Ok(_) => {
                    println!("Inserted Key {}", splitted_instructions[1])
                }
                Err(err) => {
                    eprintln!("Err: {:?}", err);
                }
            },
            CommandType::GetKeys => {
                println!("{:?}", db.data.keys());
            }
            CommandType::Get => match db.handle_get(&splitted_instructions) {
                Ok(val) => {
                    println!("{:?}", val)
                }
                Err(err) => {
                    eprintln!("Err: {:?}", err);
                    continue;
                }
            },
            CommandType::Delete => {
                if let Err(err) = db.handle_delete(&splitted_instructions) {
                    eprintln!("Err: {:?}", err);
                    continue;
                }
            }
        }
    }
}
