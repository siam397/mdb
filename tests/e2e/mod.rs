use std::fs;
use std::path::Path;
use std::time::Duration;
use std::thread;

mod test_data;
mod scenarios;
mod utils;

use crate::db::DB;
use crate::storage_engine::sstable_engine::SSTableEngine;

// Test constants
const TEST_WAL_DIR: &str = "test_wal";
const TEST_SSTABLE_DIR: &str = "test_sstables";
const FLUSH_INTERVAL_MS: u64 = 100;

fn setup_test_env() {
    // Clean up any existing test directories
    for dir in [TEST_WAL_DIR, TEST_SSTABLE_DIR] {
        if Path::new(dir).exists() {
            fs::remove_dir_all(dir).expect("Failed to clean test directory");
        }
        fs::create_dir(dir).expect("Failed to create test directory");
    }
}

fn teardown_test_env() {
    for dir in [TEST_WAL_DIR, TEST_SSTABLE_DIR] {
        if Path::new(dir).exists() {
            fs::remove_dir_all(dir).expect("Failed to clean test directory");
        }
    }
}

fn create_test_db() -> DB {
    DB::new(
        SSTableEngine::new(TEST_SSTABLE_DIR),
        TEST_WAL_DIR,
        FLUSH_INTERVAL_MS,
    )
}