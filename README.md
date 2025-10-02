# MSSQL-Debezium-Kafka-Flink-PostgreSQL Pipeline

## Quick Start

### Prerequisites
- Your existing infrastructure: Kafka, PostgreSQL, SQL Server, Debezium, Flink cluster
- Maven installed

### Step 1: Build the Project
```bash
mvn clean package -DskipTests
```

### Step 2: Copy JAR to Flink Container
```bash
docker cp target/postgresSync-0.0.1-SNAPSHOT.jar jobmanager:/opt/flink/usrlib/
```

### Step 3: Submit the Flink Job
```cmd
scripts\submit-job.bat
```

### Step 4: Monitor
- Flink Web UI: http://localhost:8081
- Kafka UI: http://localhost:8080

## Configuration

The job uses these default settings:
- Kafka: `kafka:9092`
- Topic: `sqlserver1.sourceDBMSSQL.dbo.customers`
- Database: `postgres:5432/targetDBPostgres`
- User: `debezium`
- Password: `dbz`

## Troubleshooting

1. **Job fails to start**: Check Flink logs at http://localhost:8081
2. **No data flowing**: Verify Debezium connector is running and producing to Kafka
3. **Database connection issues**: Ensure PostgreSQL is accessible from Flink containers
