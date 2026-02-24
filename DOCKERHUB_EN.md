# NineData DBBench

**Ready-to-Use TPC-C Database Benchmark Tool**

DBBench is a professional database performance testing tool that fully implements the TPC-C benchmark specification. It supports 12 major databases and provides a real-time web monitoring dashboard.

## Image Highlights

- **Multi-Database Support**: MySQL, PostgreSQL, Oracle, SQL Server, DB2, TiDB, OceanBase, Dameng, SQLite, YashanDB, Sybase, SAP HANA
- **Real-Time Monitoring**: Web UI with live TPS, latency, CPU, memory, network metrics and time range selector (All / 1m / 10m / 1h / 6h / 1day / Custom)
- **Full TPC-C**: All 5 transaction types (New-Order, Payment, Order-Status, Delivery, Stock-Level)
- **CSV Fast Load**: Database-native bulk import (MySQL LOAD DATA, PostgreSQL COPY, etc.) for faster data loading
- **Flexible Configuration**: Environment variable support for various testing scenarios

## Quick Start

### Connect to MySQL

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=mysql \
  -e DB_JDBC_URL="jdbc:mysql://host.docker.internal:3306/tpcc?useSSL=false&rewriteBatchedStatements=true" \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=yourpassword \
  --name dbbench yzsind/dbbench:latest
```

Open http://localhost:1929 in your browser to start testing.

### Connect to PostgreSQL

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=postgresql \
  -e DB_JDBC_URL="jdbc:postgresql://host.docker.internal:5432/tpcc" \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=yourpassword \
  --name dbbench yzsind/dbbench:latest
```

### Connect to Oracle

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=oracle \
  -e DB_JDBC_URL="jdbc:oracle:thin:@host.docker.internal:1521:orcl" \
  -e DB_USERNAME=system \
  -e DB_PASSWORD=yourpassword \
  --name dbbench yzsind/dbbench:latest
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_TYPE` | Database type | mysql |
| `DB_JDBC_URL` | JDBC connection URL | jdbc:mysql://host.docker.internal:3306/tpcc |
| `DB_USERNAME` | Database username | root |
| `DB_PASSWORD` | Database password | (empty) |
| `DB_POOL_SIZE` | Connection pool size | 50 |
| `BENCHMARK_WAREHOUSES` | Number of warehouses (data scale) | 10 |
| `BENCHMARK_TERMINALS` | Concurrent terminals | 50 |
| `BENCHMARK_DURATION` | Test duration in seconds | 60 |
| `BENCHMARK_LOAD_CONCURRENCY` | Data loading threads | 4 |
| `BENCHMARK_CSV_LOAD` | Use CSV bulk loading (true/false) | false |
| `JAVA_OPTS` | JVM options | -Xms512m -Xmx1024m |

## Supported Databases

| Database | JDBC URL Format                                                 |
|----------|-----------------------------------------------------------------|
| MySQL | `jdbc:mysql://host:3306/database`                               |
| PostgreSQL | `jdbc:postgresql://host:5432/database`                          |
| Oracle | `jdbc:oracle:thin:@host:1521:sid`                               |
| SQL Server | `jdbc:sqlserver://host:1433;databaseName=db`                    |
| DB2 | `jdbc:db2://host:50000/database`                                |
| TiDB | `jdbc:mysql://host:4000/database`                               |
| OceanBase | `jdbc:oceanbase://host:2881/database`                           |
| Dameng | `jdbc:dm://host:5236/database`                                  |
| SQLite | `jdbc:sqlite:./tpcc.db`                                         |
| YashanDB | `jdbc:yasdb://host:1688/database`                               |
| Sybase | `jdbc:jtds:sybase://host:5000/database`                         |
| SAP HANA | `jdbc:sap://host:30015/?currentschema=schemaName`               |

## Usage Workflow

1. **Start Container**: Run the docker run command
2. **Access Web UI**: Open http://localhost:1929
3. **Configure Connection**: Set database connection in the config panel
4. **Load Data**: Click "Load Data" to generate test data
5. **Run Benchmark**: Click "Start" to begin the benchmark
6. **View Results**: Monitor TPS, latency, and system resources in real-time, use time range selector to view different periods
7. **Export Report**: Click "Report" after test completes to view/export the benchmark report

## Data Scale Reference

Each warehouse contains approximately:
- Customer: 30,000 rows
- Order: 30,000 rows
- Order-Line: ~300,000 rows
- Stock: 100,000 rows

**Recommended Configurations:**
- Quick Test: 1-2 warehouses, 5-10 terminals
- Standard Test: 10 warehouses, 50 terminals
- Stress Test: 100+ warehouses, 200+ terminals

## Port Reference

| Port | Purpose |
|------|---------|
| 1929 | Web UI and REST API |

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/benchmark/config` | GET/POST | Get/Update configuration |
| `/api/benchmark/test-connection` | POST | Test database connection |
| `/api/benchmark/load` | POST | Load test data |
| `/api/benchmark/start` | POST | Start benchmark |
| `/api/benchmark/stop` | POST | Stop benchmark |
| `/api/benchmark/status` | GET | Get current status |
| `/api/metrics/current` | GET | Get real-time metrics |
| `/api/metrics/history` | GET | Get metrics history (up to 3600 snapshots) |
| `/api/metrics/tps-history` | GET | Get TPS history data |
| `/api/report/markdown` | GET | Get benchmark report (Markdown) |

## Metrics Collected

### Transaction Metrics
- Throughput (TPS / TPM / tpmC)
- Total transactions count
- Success / Rollback / Error counts and rates
- Latency (average, min, max)
- Per-transaction type breakdown

### Database Host Metrics (via SSH)
- CPU usage (%)
- Memory usage (%)
- Disk I/O (read/write bytes/sec)
- Network I/O (recv/sent bytes/sec)

### Database Metrics
- Active connections
- Buffer pool / Cache hit ratio
- Row lock waits
- Slow queries count

### Client Metrics (Local Machine)
- CPU usage (%)
- Memory usage (%)
- Load average (1m, 5m, 15m)
- Network I/O (bytes/sec)
- Disk I/O (bytes/sec)


## CLI Mode

In addition to the Web UI, CLI mode is also supported using configuration files:

```bash
# Enter container
docker exec -it dbbench bash

# Load data
java -jar /app/dbbench.jar -f /app/profiles/config.properties load

# Run benchmark
java -jar /app/dbbench.jar -f /app/profiles/config.properties run

# Combine actions: clean → load → run
java -jar /app/dbbench.jar -f /app/profiles/config.properties clean load run

# Override specific settings
java -jar /app/dbbench.jar -f /app/profiles/config.properties -w 20 -c 100 -d 300 run
```

## Source Code & Documentation

- GitHub: https://github.com/yzsind/dbbench
- Issues: https://github.com/yzsind/dbbench/issues

## License

Apache License 2.0
