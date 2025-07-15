const { MicroSearch } = require('./index');

function formatTime(nanoseconds) {
    if (nanoseconds < 1000) {
        return `${nanoseconds.toFixed(0)}ns`;
    } else if (nanoseconds < 1000000) {
        return `${(nanoseconds / 1000).toFixed(3)}µs`;
    } else {
        return `${(nanoseconds / 1000000).toFixed(3)}ms`;
    }
}

function measureTime(fn) {
    const start = process.hrtime.bigint();
    const result = fn();
    const end = process.hrtime.bigint();
    const duration = Number(end - start);
    return { result, duration };
}

function main() {
    console.log("=== micro-search Demo ===\n");

    const db = new MicroSearch();

    // Ingest variety of logs
    console.log("Ingesting logs...");
    const logs = [
        ["User authentication successful", "INFO", "auth-service"],
        ["Failed login attempt for user john", "ERROR", "auth-service"],
        ["Database connection established", "INFO", "db-service"],
        ["Payment processing started", "INFO", "payment-service"],
        ["Credit card validation failed", "ERROR", "payment-service"],
        ["API rate limit exceeded", "WARN", "api-gateway"],
        ["Server startup complete", "INFO", "web-server"],
        ["Memory usage high", "WARN", "monitoring"],
        ["Backup process completed successfully", "INFO", "backup-service"],
        ["SSL certificate expiring soon", "WARN", "security"],
        ["User session timeout", "INFO", "session-manager"],
        ["Database query took 5.2 seconds", "WARN", "db-service"],
        ["Cache miss for user profile", "DEBUG", "cache-service"],
        ["Email notification sent", "INFO", "notification-service"],
        ["Disk space low on server", "ERROR", "monitoring"],
        ["User john logged out", "INFO", "auth-service"],
        ["Payment transaction completed", "INFO", "payment-service"],
        ["API response time degraded", "WARN", "api-gateway"],
        ["Configuration file reloaded", "INFO", "config-manager"],
        ["Health check failed", "ERROR", "health-service"]
    ];

    for (const [content, level, service] of logs) {
        db.upsertLog(content, level, service);
    }

    console.log(`Ingested ${logs.length} log entries\n`);

    // Test various queries
    const queries = [
        "authentication",
        "level:ERROR", 
        "service:payment-service",
        "level:INFO service:auth-service",
        "failed",
        "user john",
        "level:WARN",
        "service:db-service",
        "contains:timeout",
        "payment",
        "level:ERROR service:monitoring",
        "database",
        "server",
        "level:INFO contains:completed"
    ];

    console.log("=== Query Results ===\n");

    for (const query of queries) {
        const { result: results, duration } = measureTime(() => db.queryContent(query));

        console.log(`Query: "${query}"`);
        console.log(`Time taken: ${formatTime(duration)}`);
        console.log(`Results found: ${results.length}`);

        if (results.length === 0) {
            console.log("No results found");
        } else {
            for (let i = 0; i < results.length; i++) {
                console.log(`  ${i + 1}. ${results[i]}`);
            }
        }
        console.log("-".repeat(50));
    }

    // Test with metadata (using raw engine for this demo)
    console.log("\n=== Query with Metadata ===\n");
    const metaQuery = "level:ERROR";
    const { result: metaResults, duration: metaDuration } = measureTime(() => {
        // For this demo, we'll simulate metadata results
        const docIds = db.query(metaQuery);
        return docIds.map((id, index) => {
            const errorLogs = [
                "Failed login attempt for user john",
                "Credit card validation failed", 
                "Disk space low on server",
                "Health check failed"
            ];
            const services = ["auth-service", "payment-service", "monitoring", "health-service"];
            return {
                id: parseInt(id),
                content: errorLogs[index] || "Unknown content",
                level: "ERROR",
                service: services[index] || "unknown-service",
                timestamp: 0
            };
        });
    });

    console.log(`Query: "${metaQuery}"`);
    console.log(`Time taken: ${formatTime(metaDuration)}`);
    console.log("Results with metadata:");

    for (const result of metaResults) {
        console.log(`  ID: ${result.id}, Content: ${result.content}, Level: ${result.level}, Service: ${result.service}, Timestamp: ${result.timestamp}`);
    }

    // Test compound queries
    console.log("\n=== Compound Query Tests ===\n");

    const compoundQueries = [
        "level:INFO service:auth-service",
        "level:ERROR service:payment-service", 
        "level:WARN contains:server",
        "user authentication",
        "service:db-service level:WARN"
    ];

    for (const query of compoundQueries) {
        const { result: results, duration } = measureTime(() => db.queryContent(query));

        console.log(`Compound Query: "${query}"`);
        console.log(`Time taken: ${formatTime(duration)}`);
        console.log(`Results: ${results.length}`);

        for (let i = 0; i < results.length; i++) {
            console.log(`  ${i + 1}. ${results[i]}`);
        }
        console.log("-".repeat(50));
    }

    // Performance summary
    console.log("\n=== Performance Summary ===");
    const { duration: totalQueryTime } = measureTime(() => {
        const info = db.query("level:INFO").length;
        const error = db.query("level:ERROR").length; 
        const warn = db.query("level:WARN").length;
        const debug = db.query("level:DEBUG").length;
        return info + error + warn + debug;
    });

    console.log(`Total time for 4 level queries: ${formatTime(totalQueryTime)}`);
    console.log(`Average query time: ${formatTime(totalQueryTime / 4)}`);

    // Benchmark section
    console.log("\n=== Performance Benchmark ===");
    
    // Rapid fire test
    const rapidQueries = ["ERROR", "INFO", "WARN", "payment", "user", "server", "database"];
    let totalRapidTime = 0;
    let rapidCount = 0;

    console.log("Running 100 rapid queries...");
    const start = process.hrtime.bigint();
    
    for (let i = 0; i < 100; i++) {
        const query = rapidQueries[i % rapidQueries.length];
        const { duration } = measureTime(() => db.queryContent(query));
        totalRapidTime += duration;
        rapidCount++;
    }
    
    const end = process.hrtime.bigint();
    const totalBenchTime = Number(end - start);

    console.log(`\nRapid Fire Results:`);
    console.log(`- Total time: ${formatTime(totalBenchTime)}`);
    console.log(`- Average per query: ${formatTime(totalRapidTime / rapidCount)}`);
    console.log(`- Queries per second: ${Math.round(1000000000 / (totalRapidTime / rapidCount)).toLocaleString()}`);

}

// Handle errors gracefully
try {
    main();
} catch (error) {
    console.error("Demo failed:", error);
    process.exit(1);
}