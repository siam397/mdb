use std::fs;
use std::path::Path;

// Utility functions for E2E tests

pub fn count_sstable_files(dir: &str) -> usize {
    if !Path::new(dir).exists() {
        return 0;
    }
    fs::read_dir(dir)
        .expect("Failed to read SSTable directory")
        .filter(|entry| {
            entry.as_ref()
                .ok()
                .and_then(|e| e.path().extension())
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "sst")
                .unwrap_or(false)
        })
        .count()
}

pub fn count_wal_files(dir: &str) -> usize {
    if !Path::new(dir).exists() {
        return 0;
    }
    fs::read_dir(dir)
        .expect("Failed to read WAL directory")
        .filter(|entry| {
            entry.as_ref()
                .ok()
                .and_then(|e| e.path().extension())
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "wal")
                .unwrap_or(false)
        })
        .count()
}

pub fn get_total_sstable_size(dir: &str) -> u64 {
    if !Path::new(dir).exists() {
        return 0;
    }
    fs::read_dir(dir)
        .expect("Failed to read SSTable directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "sst")
                .unwrap_or(false)
        })
        .map(|entry| entry.metadata().map(|m| m.len()).unwrap_or(0))
        .sum()
}