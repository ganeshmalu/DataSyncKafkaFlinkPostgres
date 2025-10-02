@echo off
REM Simple Flink Job Submission Script for Windows
REM For submitting the job to your existing Flink cluster

echo Submitting Flink job to existing cluster...

REM Check if JAR exists
if not exist "target\postgresSync-0.0.1-SNAPSHOT.jar" (
    echo Error: JAR file not found. Building project...
    mvn clean package -DskipTests
)

REM Copy JAR to Flink container
echo Copying JAR to Flink container...
docker cp target\postgresSync-0.0.1-SNAPSHOT.jar jobmanager:/opt/flink/usrlib/

REM Submit the job
echo Submitting job with parameters:
echo   Kafka: kafka:9092
echo   Topic: sqlserver1.sourceDBMSSQL.dbo.customers
echo   Database: postgres:5432/targetDBPostgres

docker exec jobmanager /opt/flink/bin/flink run --class com.baylor.postgresSync.sync.FlinkMSSQLToPostgresJob --parallelism 1 /opt/flink/usrlib/postgresSync-0.0.1-SNAPSHOT.jar

if %ERRORLEVEL% equ 0 (
    echo Job submitted successfully!
    echo Monitor at: http://localhost:8081
) else (
    echo Job submission failed
    exit /b 1
)
