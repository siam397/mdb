use std::{fs, sync::Arc};

use tokio::time::{Duration, sleep};

use crate::{storage_engine::engine::Engine, wal::Wal};

pub struct Flusher<E: Engine + 'static + Send + Sync> {
    wal: Arc<Wal<E>>,
    storage_engine: Arc<E>,
    flush_interval_secs: u64,
}

impl<E: Engine + Send + Sync + 'static> Flusher<E> {
    pub fn new(flush_interval_secs: u64, wal: Arc<Wal<E>>, storage_engine: Arc<E>) -> Self {
        Flusher {
            flush_interval_secs,
            wal,
            storage_engine,
        }
    }

    pub fn start(&self) {
        let interval = self.flush_interval_secs;
        let wal_clone = self.wal.clone();
        let storage_engine = self.storage_engine.clone();
        let mut flush_count = 0;
        println!("Flusher started");
        tokio::spawn(async move {
            loop {
                let files = wal_clone.get_wal_files_available_for_snapshot().unwrap();

                println!("available files found for flush {}", files.len());

                match wal_clone.play_wal_to_store() {
                    Ok(_) => {}
                    Err(e) => println!("{:?}", e),
                };

                for file in files {
                    match fs::remove_file(&file) {
                        Ok(_) => (),
                        Err(_) => println!("Failed to delete file {}", file),
                    }
                }

                flush_count += 1;
                if flush_count >= 2 {
                    // Run compaction every 2 flushes
                    match storage_engine.compact_sstables() {
                        Ok(_) => println!("SSTable compaction completed successfully"),
                        Err(e) => println!("SSTable compaction failed: {:?}", e),
                    }
                    flush_count = 0; // Reset counter
                }

                sleep(Duration::from_secs(interval)).await;
            }
        });
    }
}
