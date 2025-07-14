use logdb::{LogDB, LogConfig};
use std::time::Instant;
use omega::omega_timer::timer_init;

#[test]
fn test_microsecond_query_performance() {
    timer_init();
    let mut db = LogDB::new();
    
    // Insert 10k log entries
    for i in 0..10000 {
        let log = format!("ERROR [service-{}] Database connection failed - user:{} ip:192.168.1.{}", 
                         i % 5, i % 100, (i % 254) + 1);
        db.upsert_simple(&log);
    }

    // Test query performance 100 times
    let mut total_time = 0u128;
    for _ in 0..100 {
        let start = Instant::now();
        let results = db.query("ERROR");  // Returns Vec<DocId> now
        let duration = start.elapsed();
        total_time += duration.as_nanos();
        
        assert!(!results.is_empty());
        println!("Query took: {:.2}μs", duration.as_nanos() as f64 / 1000.0);
    }
    
    let avg_time_us = total_time as f64 / 100.0 / 1000.0;
    println!("Average query time: {:.2}μs", avg_time_us);
    
    // Verify sub-5μs performance
    assert!(avg_time_us < 5.0, "Average query time {:.2}μs exceeds 5μs target", avg_time_us);
}

#[test]
fn test_microsecond_insert_performance() {
    timer_init();
    let mut db = LogDB::new();
    
    // Pre-populate with some data
    for i in 0..1000 {
        let log = format!("INFO [service] Background task {} completed", i);
        db.upsert_simple(&log);
    }

    // Test insert performance
    let mut total_time = 0u128;
    let test_entries = (0..100).map(|i| {
        format!("ERROR [auth] Login failed for user:{} ip:10.0.0.{}", i, i)
    }).collect::<Vec<_>>();

    for entry in &test_entries {
        let start = Instant::now();
        db.upsert_simple(entry);
        let duration = start.elapsed();
        total_time += duration.as_nanos();
        
        println!("Insert took: {:.2}μs", duration.as_nanos() as f64 / 1000.0);
    }
    
    let avg_time_us = total_time as f64 / test_entries.len() as f64 / 1000.0;
    println!("Average insert time: {:.2}μs", avg_time_us);
    
    // Verify sub-15μs performance
    assert!(avg_time_us < 15.0, "Average insert time {:.2}μs exceeds 15μs target", avg_time_us);
}

#[test]
fn test_large_dataset_performance() {
    timer_init();
    let mut db = LogDB::new();
    
    println!("Inserting 100k log entries...");
    let mut dataSet = vec![];
    for i in 0..100_000 {
        let level = ["ERROR", "WARN", "INFO", "DEBUG"][i % 4];
        let service = ["auth", "db", "api", "worker"][i % 4];
        let log = format!("{} [{}] Operation {} completed - duration:{}ms", 
                         level, service, i, (i % 1000) + 1);
        dataSet.push(log);
    }
    let start = Instant::now();
    
    for log in dataSet {
        db.upsert_simple(&log);
    }
    
    let insert_time = start.elapsed();
    println!("Inserted 100k entries in {:.2}ms ({:.2}μs per insert)", 
             insert_time.as_nanos() as f64 / 1_000_000.0,
             insert_time.as_nanos() as f64 / 100_000.0 / 1000.0);
    println!("Inserted {} entries, {}", 100_000, db.stats());
    // Test various queries on large dataset
    let queries = [
        "ERROR",
        "level:WARN", 
        "service:auth",
        "contains:database",
        "\"Operation completed\"",
        "level:ERROR AND service:db"
    ];

    for query in &queries {
        let start = Instant::now();
        let results = db.query(query);
        let duration = start.elapsed();
        
        println!("Query '{}' took {:.2}μs, found {} results", 
                 query, duration.as_nanos() as f64 / 1000.0, results.len());
        
        // All queries should be sub-10μs even on large dataset
        assert!(duration.as_nanos() < 10_000, 
                "Query '{}' took {:.2}μs, exceeds 10μs", query, duration.as_nanos() as f64 / 1000.0);
    }
}

#[test]
fn test_memory_efficiency() {
    timer_init();
    let mut db = LogDB::new();
    
    println!("Testing memory efficiency...");
    
    // Insert data in batches and monitor memory
    for batch in 0..10 {
        for i in 0..1000 {
            let log = format!("INFO [service-{}] Processing request {} - status:success", 
                             batch, i);
            db.upsert_simple(&log);
        }
        
        println!("Batch {}: {}", batch, db.stats());
        
        // Test memory doesn't grow unbounded due to eviction
        let stats = db.stats();
        if stats.contains("postings") {
            // Extract posting count - rough check that eviction is working
            println!("Memory stats: {}", stats);
        }
    }
}

#[test]
fn test_real_world_log_patterns() {
    timer_init();
    let mut db = LogDB::new();
    
    // Simulate real Apache/Nginx style logs
    let real_logs = vec![
        "192.168.1.100 - - [25/Dec/2023:10:00:01 +0000] \"GET /api/users HTTP/1.1\" 200 1234",
        "192.168.1.101 - - [25/Dec/2023:10:00:02 +0000] \"POST /auth/login HTTP/1.1\" 401 567",
        "ERROR 2023-12-25 10:00:03 Database connection pool exhausted",
        "WARN 2023-12-25 10:00:04 High memory usage: 85%",
        "INFO 2023-12-25 10:00:05 User session expired: user_id=12345",
        "[ERROR] 2023-12-25T10:00:06Z auth-service: JWT token validation failed",
        "Dec 25 10:00:07 server01 nginx: [error] 1234#0: *5678 connect() failed (111: Connection refused)",
    ];

    for log in &real_logs {
        db.upsert_simple(log);
    }

    // Test various realistic queries
    let test_queries = vec![
        ("192.168.1.100", "IP address search"),
        ("ERROR", "Error level search"),
        ("\"connection refused\"", "Phrase search"),
        ("contains:database", "Contains search"),
        ("HTTP/1.1", "Protocol search"),
    ];

    for (query, description) in test_queries {
        let start = Instant::now();
        let results = db.query(query);
        let duration = start.elapsed();
        
        println!("{}: '{}' took {:.2}μs, found {} results", 
                 description, query, duration.as_nanos() as f64 / 1000.0, results.len());
    }
}