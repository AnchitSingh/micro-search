# ⚡ micro-search ⚡

> ### A high-performance, in-memory search engine for Node.js.

**3μs queries. 400,000+ QPS. Zero infrastructure.**

`micro-search` delivers **instant search** for your Node.js applications. Built with Rust and powered by the Buggu engine, it provides real-time search capabilities without the need for external databases or services.

```javascript
const { MicroSearch } = require('micro-search');

const db = new MicroSearch();
db.upsertLog("User authentication failed", "ERROR", "auth-service");
db.upsertLog("Payment processed successfully", "INFO", "payment-service");

const errors = db.query("level:ERROR");        // 3μs response ⚡
const authLogs = db.queryContent("service:auth"); // Lightning fast 🔥
```

---

## 🚀 Why micro-search?

### **Instant Results**
- **~3μs average query time**—often faster than memory allocation.
- **400,000+ queries per second** on a single core.
- **Sub-millisecond** performance, even with complex Boolean queries.

### **Zero Infrastructure**
- **One command install:** `npm install micro-search`.
- **No setup** or configuration required.
- **No external dependencies**—it's pure Node.js and Rust.

### **Developer Friendly**
- **Intuitive API** that feels like a simple key-value store.
- **Rich query language**, including `level:ERROR AND service:auth`.
- **Full TypeScript support** with detailed type definitions.

---

## 📊 Performance Comparison

| Search Engine | Query Time | Setup Time | Infrastructure Cost |
|---------------|------------|------------|---------------------|
| **micro-search** | **3μs** ⚡ | **30 seconds** | **$0** |
| Database `LIKE` | 200ms+ 🐌 | Hours | $200+/month |
| `grep` / `awk` | 5000ms+ 🐌 | Minutes | Server costs |

*For indexed queries, micro-search is orders of magnitude faster than command-line tools.*

---

## 🎯 Perfect For

### **📊 Real-time Dashboards**
```javascript
// Admin dashboard showing live errors
const recentErrors = db.query("level:ERROR");
const serviceHealth = db.queryContent("service:payment");
```

### **🔍 Log Analysis**
```javascript
// Debug user issues instantly
const userLogs = db.query("user:john AND level:ERROR");
const timeouts = db.query("contains:timeout");
```

### **⚡ Live Search & Autocomplete**
```javascript
// Search-as-you-type with zero lag
app.get('/search', (req, res) => {
  const results = db.query(req.query.q);
  res.json(results);
});
```

### **📱 Embedded App Search**
```javascript
// In-app content search
const docs = db.query("contains:authentication");
const help = db.query("category:help AND priority:high");
```

---

## 🛠️ Installation

```bash
npm install micro-search
```

**Supports:** Linux (x64, ARM64), macOS (Intel, Apple Silicon), and Windows (x64).

---

## ⚡ Quick Start

### **Basic Usage**
```javascript
const { MicroSearch } = require('micro-search');

const db = new MicroSearch();

// Add some data
db.upsertSimple("Server started successfully");
db.upsertLog("Database connection failed", "ERROR", "db-service");
db.upsertLog("User login successful", "INFO", "auth-service");

// Search instantly
const errorIds = db.query("level:ERROR");
const dbServiceContent = db.queryContent("service:db-service");
const connectionContent = db.queryContent("contains:connection");
```

---

## Advanced Queries

The query engine supports boolean logic for creating complex and precise queries.

### **Boolean Operators**
-   **`AND`**: (Default) Narrows the search. `level:ERROR service:auth` is the same as `level:ERROR AND service:auth`.
-   **`OR`**: Broadens the search. `level:ERROR OR level:WARN` finds documents with either log level.
-   **`NOT`**: Excludes results. `service:auth NOT "login successful"` finds all logs from the auth service except those indicating a successful login.

```javascript
// Find all errors from the payment service
db.query("level:ERROR AND service:payment");

// Find all logs that are either errors or warnings
db.query("level:ERROR OR level:WARN");

// Find all logs that do not contain the word "success"
db.query("NOT contains:success");
```

### **Range Queries (Planned)**

The query parser is designed to recognize numeric range syntax for the `timestamp` field (e.g., `timestamp:>=167...`). However, the query execution logic for these ranges is not yet implemented.

This feature is planned for a future release. Currently, range queries will parse correctly but will not return results.

---

## 📚 API Reference

### **`new MicroSearch()`**
Creates a new search instance.

### **`.upsertSimple(content: string): string`**
Adds simple text to the index. Returns the document ID.

### **`.upsertLog(content: string, level?: string, service?: string): string`**
Adds a structured log entry. `level` and `service` are optional. Returns the document ID.

### **`.query(queryString: string): string[]`**
Searches the index and returns an array of matching document IDs.

### **`.queryContent(queryString: string): string[]`**
Searches the index and returns an array of the full content of matching documents.

### **Query Language**
| Query | Description | Example |
|-------|-------------|---------|
| `text` | Simple text search | `"timeout"` |
| `level:VALUE` | Filter by log level | `level:ERROR` |
| `service:VALUE` | Filter by service | `service:auth` |
| `contains:VALUE` | Text contains | `contains:database` |
| `"exact phrase"` | Exact phrase match | `"connection failed"` |
| `AND` / `OR` / `NOT` | Boolean logic | `level:ERROR AND NOT service:payment` |

---

## 🚀 Benchmarks

Run the included benchmarks to see the performance on your machine:

```bash
npm run bench
```

---

## 🔥 Use Cases

### **DevOps & Monitoring**
- **Log aggregation** without heavy infrastructure.
- **Real-time error monitoring** and alerting.
- **Service health dashboards** with live data.
- **Instant alert correlation** during incidents.

### **Web Applications**
- **Admin panel search** for users, orders, or content.
- **User-generated content search** (e.g., comments, posts).
- **Support ticket search** and analysis.
- **Live chat message search**.

---

## 💡 Why So Fast?

`micro-search` is built on the **Buggu engine** with several breakthrough optimizations:

- **🔥 OmegaHashSet:** A custom hash table that is up to 40x faster than standard implementations.
- **⚡ Zero-copy Tokenization:** Minimizes memory allocations during indexing.
- **🎯 Optimized Set Operations:** Microsecond-fast intersections for complex queries.
- **📊 Smart Indexing:** Efficient inverted indices for instant lookups.
- **🛠️ Rust Core:** Memory-safe, native performance at its best.

---

## 🤝 Contributing

We welcome contributions! Here’s how to get started:

```bash
git clone https://github.com/AnchitSingh/micro-search.git
cd micro-search
npm install
npm test
```

### **Development**
- **Rust Core:** `src/`
- **Node.js Wrapper:** `index.js`
- **Tests:** `test.js`
- **Benchmarks:** `benchmark.js`

---

## 📄 License

MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🌟 Star Us!

If `micro-search` saves you time and infrastructure costs, please give us a star! ⭐

**Made with ❤️ and Rust. Powered by the Buggu engine.**
