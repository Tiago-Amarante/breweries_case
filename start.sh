#!/bin/bash

# Data Engineering Architecture Startup Script
echo "Starting Data Engineering Architecture..."

# Ensure the required directories exist
echo "Creating required directories..."
mkdir -p logs plugins spark_files
mkdir -p spark_files/data
mkdir -p spark_files/jars

# Create a directory for local Spark data
echo "Creating directory for local Spark data processing..."
mkdir -p spark_data

# Download required JAR files
echo "Downloading required JAR files..."
cd spark
chmod +x download_jars.sh
./download_jars.sh
cd ..

# Start the services
echo "Starting Docker Compose services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start (this may take a few minutes)..."
sleep 10

echo "Checking service health..."
AIRFLOW_READY=false
SPARK_READY=false
MINIO_READY=false

# Checking Airflow
for i in {1..12}; do
  if curl -s http://localhost:8080 > /dev/null; then
    AIRFLOW_READY=true
    break
  fi
  echo "Waiting for Airflow Web UI to be ready..."
  sleep 10
done

# Checking Spark
for i in {1..12}; do
  if curl -s http://localhost:8181 > /dev/null; then
    SPARK_READY=true
    break
  fi
  echo "Waiting for Spark Master UI to be ready..."
  sleep 10
done

# Checking MinIO
for i in {1..12}; do
  if curl -s http://localhost:9000/minio/health/live > /dev/null; then
    MINIO_READY=true
    break
  fi
  echo "Waiting for MinIO to be ready..."
  sleep 10
done

# Create spark_data directory in Airflow container for local processing
echo "Creating spark_data directory in Airflow containers..."
docker exec breweries_case_airflow-webserver_1 mkdir -p /opt/airflow/spark_data
docker exec breweries_case_airflow-worker_1 mkdir -p /opt/airflow/spark_data
docker exec breweries_case_airflow-scheduler_1 mkdir -p /opt/airflow/spark_data

# Final output
echo ""
echo "==================================================="
echo "Data Engineering Architecture Status"
echo "==================================================="
echo "Airflow Web UI: $([ "$AIRFLOW_READY" = true ] && echo "READY" || echo "NOT READY")"
echo "Spark Master UI: $([ "$SPARK_READY" = true ] && echo "READY" || echo "NOT READY")"
echo "MinIO: $([ "$MINIO_READY" = true ] && echo "READY" || echo "NOT READY")"
echo ""
echo "Access URLs:"
echo "- Airflow: http://localhost:8080 (user: airflow, password: airflow)"
echo "- Spark Master: http://localhost:8181"
echo "- MinIO Console: http://localhost:9001 (user: minioadmin, password: minioadmin)"
echo ""
echo "To view logs: docker-compose logs -f [service_name]"
echo "To stop all services: docker-compose down"
echo "===================================================" 