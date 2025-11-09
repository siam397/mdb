pub mod common;
pub mod db;
pub mod ende;
pub mod flusher;
pub mod storage_engine;
pub mod wal;
use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};


use crate::{
    common::command_type::CommandType,
    db::Db,
    flusher::Flusher,
    storage_engine::{engine::Engine, sstable_engine::SSTableEngine},
    wal::Wal,
};
#[tokio::main]
async fn main() {
    println!("Welcome to MiniDB (TCP Mode)");

    // Shared storage engine
    let storage_engine = Arc::new(SSTableEngine::new(String::from("data")));
    let flusher_wal = Wal::new(String::from("wal"), SSTableEngine::new(String::from("data")));

    let flusher = Flusher::new(40, Arc::new(flusher_wal), storage_engine.clone());
    flusher.start();

    // Shared db between clients
    let sstable_engine = SSTableEngine::new(String::from("data"));
    let wal = Wal::new(String::from("wal"), SSTableEngine::new(String::from("data")));
    let db = Arc::new(tokio::sync::Mutex::new(
        Db::new(sstable_engine, wal).expect("Failed to load db"),
    ));

    // Listen on port 4000
    let listener = TcpListener::bind("127.0.0.1:4000")
        .await
        .expect("Failed to bind port 4000");

    println!("Listening on 127.0.0.1:4000 ...");

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("Client connected: {}", addr);

        let db_clone = db.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                line.clear();
                let bytes_read = reader.read_line(&mut line).await.unwrap();
                if bytes_read == 0 {
                    println!("Client {} disconnected", addr);
                    break;
                }

                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = input.split_whitespace().collect();
                if parts.is_empty() {
                    writer.write_all(b"Invalid command\n").await.unwrap();
                    continue;
                }

                let command_type = match CommandType::command_type_from_str(parts[0]) {
                    Some(c) => c,
                    None => {
                        writer.write_all(b"Invalid command\n").await.unwrap();
                        continue;
                    }
                };

                let mut db = db_clone.lock().await;

                match command_type {
                    CommandType::Set => match db.handle_set(&parts) {
                        Ok(_) => {
                            writer
                                .write_all(format!("OK: inserted {}\n", parts[1]).as_bytes())
                                .await
                                .unwrap();
                        }
                        Err(e) => {
                            writer
                                .write_all(format!("ERR: {:?}\n", e).as_bytes())
                                .await
                                .unwrap();
                        }
                    },
                    CommandType::Get => match db.handle_get(&parts) {
                        Ok(val) => {
                            writer
                                .write_all(format!("{:?}\n", val).as_bytes())
                                .await
                                .unwrap();
                        }
                        Err(e) => {
                            writer
                                .write_all(format!("ERR: {:?}\n", e).as_bytes())
                                .await
                                .unwrap();
                        }
                    },
                    CommandType::GetKeys => {
                        writer
                            .write_all(
                                format!("{:?}\n", db.data.keys().collect::<Vec<_>>()).as_bytes(),
                            )
                            .await
                            .unwrap();
                    }
                    CommandType::Delete => match db.handle_delete(&parts) {
                        Ok(_) => {
                            writer.write_all(b"OK: deleted\n").await.unwrap();
                        }
                        Err(e) => {
                            writer
                                .write_all(format!("ERR: {:?}\n", e).as_bytes())
                                .await
                                .unwrap();
                        }
                    },
                }
            }
        });
    }
}
