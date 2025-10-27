use std::{
    sync::{Arc, Mutex},
};

use crate::{
    common::db_errors::DbError,
    storage_engine::engine::Engine,
    wal::Wal,
};

pub struct Flusher<E: Engine> {
    wal: Arc<Mutex<Wal<E>>>,
    flush_interval_secs: u64,
}

impl<E: Engine + 'static> Flusher<E> {
    pub fn new(wal: Wal<E>, flush_interval_secs: u64) -> Self {
        Flusher {
            wal: Arc::new(Mutex::new(wal)),
            flush_interval_secs,
        }
    }

    /// Start the periodic flushing in a background thread
    // pub fn start(&self) -> thread::JoinHandle<()> {
    //     let wal = Arc::clone(&self.wal);
    //     let interval = self.flush_interval_secs;

    // }

    /// Get a reference to the WAL for manual operations
    pub fn get_wal(&self) -> Arc<Mutex<Wal<E>>> {
        Arc::clone(&self.wal)
    }

    /// Manually trigger a flush
    pub fn flush_now(&self) -> Result<(), DbError> {
        let wal = self.wal.lock().unwrap();
        
        let files = wal.get_wal_files_available_for_snapshot()?;
        
        if files.is_empty() {
            println!("[Flusher] No files to flush");
            return Ok(());
        }

        println!("[Flusher] Flushing {} WAL files", files.len());
        for file in &files {
            println!("  - {}", file);
        }

        // TODO: Call actual flush method when implemented
        // wal.flush_wal_to_sstable()?;

        Ok(())
    }
}