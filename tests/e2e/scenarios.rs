use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;
use crate::test_data::TestData;
use crate::{setup_test_env, teardown_test_env, create_test_db};

#[test]
fn test_basic_operations_with_flush() {
    setup_test_env();
    let db = create_test_db();
    
    // Generate test data
    let test_records = TestData::generate_batch(100);
    
    // Insert records
    for record in &test_records {
        db.insert(&record.key, &record.value)
            .expect("Failed to insert record");
    }
    
    // Wait for flush to occur
    thread::sleep(Duration::from_millis(150));
    
    // Verify all records
    for record in &test_records {
        let result = db.get(&record.key)
            .expect("Failed to get record")
            .expect("Record not found");
        assert_eq!(result, record.value, "Retrieved value doesn't match inserted value");
    }
    
    // Delete some records
    for record in test_records.iter().take(50) {
        db.delete(&record.key).expect("Failed to delete record");
    }
    
    // Wait for flush
    thread::sleep(Duration::from_millis(150));
    
    // Verify deletions
    for record in test_records.iter().take(50) {
        let result = db.get(&record.key).expect("Failed to query DB");
        assert!(result.is_none(), "Record should have been deleted");
    }
    
    teardown_test_env();
}

#[test]
fn test_wal_recovery() {
    setup_test_env();
    {
        let db = create_test_db();
        
        // Insert records
        let test_records = TestData::generate_batch(50);
        for record in &test_records {
            db.insert(&record.key, &record.value)
                .expect("Failed to insert record");
        }
        
        // Don't wait for flush - simulate crash
        drop(db);
        
        // Create new DB instance - should recover from WAL
        let recovered_db = create_test_db();
        
        // Verify records
        for record in &test_records {
            let result = recovered_db.get(&record.key)
                .expect("Failed to get record")
                .expect("Record not found after recovery");
            assert_eq!(result, record.value, "Recovered value doesn't match original");
        }
    }
    teardown_test_env();
}

#[test]
fn test_sstable_compaction() {
    setup_test_env();
    let db = create_test_db();
    
    // Insert multiple batches with pauses to trigger flushes
    for _ in 0..3 {
        let batch = TestData::generate_batch(100);
        for record in &batch {
            db.insert(&record.key, &record.value)
                .expect("Failed to insert record");
        }
        thread::sleep(Duration::from_millis(150));
    }
    
    // Verify SSTable files were created
    assert!(Path::new("test_sstables").read_dir().unwrap().count() > 0, 
           "No SSTable files were created");
    
    // Update same keys multiple times
    let update_batch = TestData::generate_batch(50);
    for _ in 0..3 {
        for record in &update_batch {
            db.insert(&record.key, &record.value)
                .expect("Failed to insert record");
        }
        thread::sleep(Duration::from_millis(150));
    }
    
    // Verify final state
    for record in &update_batch {
        let result = db.get(&record.key)
            .expect("Failed to get record")
            .expect("Record not found");
        assert_eq!(result, record.value, "Final value doesn't match last update");
    }
    
    teardown_test_env();
}

#[test]
fn test_concurrent_operations() {
    setup_test_env();
    let db = create_test_db();
    
    // Generate test data
    let test_records = TestData::generate_batch(200);
    let (write_records, read_records) = test_records.split_at(100);
    
    // Insert initial read records
    for record in read_records {
        db.insert(&record.key, &record.value)
            .expect("Failed to insert record");
    }
    
    thread::sleep(Duration::from_millis(150)); // Wait for flush
    
    // Spawn reader thread
    let read_keys: Vec<String> = read_records.iter()
        .map(|r| r.key.clone())
        .collect();
    
    let reader_handle = thread::spawn(move || {
        for _ in 0..100 {
            for key in &read_keys {
                let _ = db.get(key).expect("Failed to read");
            }
            thread::sleep(Duration::from_millis(1));
        }
    });
    
    // Perform writes in main thread
    for record in write_records {
        db.insert(&record.key, &record.value)
            .expect("Failed to insert record");
        thread::sleep(Duration::from_millis(1));
    }
    
    reader_handle.join().expect("Reader thread panicked");
    
    teardown_test_env();
}

#[test]
fn test_large_dataset_handling() {
    setup_test_env();
    let db = create_test_db();
    
    // Insert large batch of records
    let large_batch = TestData::generate_batch(1000);
    
    for (i, record) in large_batch.iter().enumerate() {
        db.insert(&record.key, &record.value)
            .expect("Failed to insert record");
        
        // Force flush every 200 records
        if i > 0 && i % 200 == 0 {
            thread::sleep(Duration::from_millis(150));
        }
    }
    
    // Verify all records while also performing updates
    for (i, record) in large_batch.iter().enumerate() {
        // Verify existing record
        let result = db.get(&record.key)
            .expect("Failed to get record")
            .expect("Record not found");
        assert_eq!(result, record.value);
        
        // Update every 10th record
        if i % 10 == 0 {
            let new_record = TestData::generate_single();
            db.insert(&record.key, &new_record.value)
                .expect("Failed to update record");
        }
    }
    
    teardown_test_env();
}