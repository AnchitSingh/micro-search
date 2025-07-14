use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use logdb::{LogDB, LogConfig};
use std::time::Instant;

fn generate_log_entries(count: usize) -> Vec<String> {
    let levels = ["ERROR", "WARN", "INFO", "DEBUG"];
    let services = ["auth", "database", "api", "worker", "cache"];
    let messages = [
        "Database connection timeout",
        "User login successful", 
        "Invalid authentication token",
        "Memory usage high",
        "Request processed successfully",
        "Cache miss for key user:123",
        "Connection pool exhausted",
        "Rate limit exceeded",
        "Background job completed",
        "SSL handshake failed"
    ];

    (0..count).map(|i| {
        let level = levels[i % levels.len()];
        let service = services[i % services.len()];
        let message = messages[i % messages.len()];
        let user_id = i % 1000;
        let ip = format!("192.168.{}.{}", (i % 254) + 1, (i % 254) + 1);
        
        format!("{} [{}] {}: {} - user:{} ip:{}", 
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                level, service, message, user_id, ip)
    }).collect()
}

fn setup_populated_db(entry_count: usize) -> LogDB {
    let mut db = LogDB::new();
    let entries = generate_log_entries(entry_count);
    
    for entry in entries {
        db.upsert_simple(&entry);
    }
    db
}

fn bench_insert_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_performance");
    
    for entry_count in [1000, 5000, 10000, 50000].iter() {
        group.bench_with_input(
            BenchmarkId::new("upsert_simple", entry_count),
            entry_count,
            |b, &count| {
                let entries = generate_log_entries(100); // Fresh entries to insert
                b.iter(|| {
                    let mut db = setup_populated_db(count);
                    let start = Instant::now();
                    for entry in &entries {
                        black_box(db.upsert_simple(entry));
                    }
                    let duration = start.elapsed();
                    println!("Average insert time: {:.2}μs", 
                             duration.as_nanos() as f64 / entries.len() as f64 / 1000.0);
                });
            },
        );
    }
    group.finish();
}

fn bench_query_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_performance");
    
    for entry_count in [1000, 5000, 10000, 50000].iter() {
        let db = setup_populated_db(*entry_count);
        
        group.bench_with_input(
            BenchmarkId::new("simple_term", entry_count),
            entry_count,
            |b, _| {
                b.iter(|| {
                    let start = Instant::now();
                    let results = black_box(db.query("ERROR"));
                    let duration = start.elapsed();
                    println!("Query 'ERROR' took: {:.2}μs, found {} results", 
                             duration.as_nanos() as f64 / 1000.0, results.len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("complex_query", entry_count),
            entry_count,
            |b, _| {
                b.iter(|| {
                    let start = Instant::now();
                    let results = black_box(db.query("level:ERROR AND contains:database"));
                    let duration = start.elapsed();
                    println!("Complex query took: {:.2}μs, found {} results", 
                             duration.as_nanos() as f64 / 1000.0, results.len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("phrase_search", entry_count),
            entry_count,
            |b, _| {
                b.iter(|| {
                    let start = Instant::now();
                    let results = black_box(db.query("\"connection timeout\""));
                    let duration = start.elapsed();
                    println!("Phrase search took: {:.2}μs, found {} results", 
                             duration.as_nanos() as f64 / 1000.0, results.len());
                });
            },
        );
    }
    group.finish();
}

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    
    group.bench_function("memory_growth", |b| {
        b.iter(|| {
            let mut db = LogDB::new();
            let entries = generate_log_entries(10000);
            
            for (i, entry) in entries.iter().enumerate() {
                db.upsert_simple(entry);
                if i % 1000 == 0 {
                    println!("After {} entries: {}", i, db.stats());
                }
            }
            black_box(db);
        });
    });
    
    group.finish();
}

fn stress_test_rapid_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("stress_test");
    
    group.bench_function("rapid_inserts_and_queries", |b| {
        b.iter(|| {
            let mut db = LogDB::new();
            let entries = generate_log_entries(1000);
            
            // Rapid fire inserts
            let insert_start = Instant::now();
            for entry in &entries {
                black_box(db.upsert_simple(entry));
            }
            let insert_duration = insert_start.elapsed();
            
            // Rapid fire queries
            let query_start = Instant::now();
            for _ in 0..100 {
                black_box(db.query("ERROR"));
                black_box(db.query("level:WARN"));
                black_box(db.query("contains:database"));
                black_box(db.query("\"user login\""));
            }
            let query_duration = query_start.elapsed();
            
            println!("Rapid test: {} inserts in {:.2}ms ({:.2}μs/insert), 400 queries in {:.2}ms ({:.2}μs/query)",
                     entries.len(),
                     insert_duration.as_nanos() as f64 / 1_000_000.0,
                     insert_duration.as_nanos() as f64 / entries.len() as f64 / 1000.0,
                     query_duration.as_nanos() as f64 / 1_000_000.0,
                     query_duration.as_nanos() as f64 / 400.0 / 1000.0);
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_performance,
    bench_query_performance, 
    bench_memory_usage,
    stress_test_rapid_operations
);
criterion_main!(benches);