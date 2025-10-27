use std::{fs, sync::Arc};

use tokio::time::{Duration, sleep};

use crate::{storage_engine::engine::Engine, wal::Wal};

pub struct Flusher<E: Engine + 'static + Send+ Sync> {
    engine: Arc<E>,
    wal: Arc<Wal<E>>,
    flush_interval_secs: u64,
}

impl <E: Engine + Send + Sync + 'static>  Flusher<E> {
    pub fn new(flush_interval_secs: u64, engine: Arc<E>, wal:Arc<Wal<E>>) -> Self {
        Flusher {
            flush_interval_secs,
            engine,
            wal
        }
    }

    pub fn start(&self) {
        let interval = self.flush_interval_secs;
        let wal_clone  = self.wal.clone();
        tokio::spawn(async move{
            loop {
                let files = wal_clone.get_wal_files_available_for_snapshot().unwrap();
                wal_clone.play_wal_to_store();

                for file in files {
                    match fs::remove_file(&file) {
                        Ok(_) => (),
                        Err(_) => println!("Failed to delete file {}", file),
                    }
                }
                
                sleep(Duration::from_secs(interval)).await;
            }
        });
    }
}
