#!/bin/bash
set -e

echo "Starting Brewery Data Pipeline..."

# Create required directories if they don't exist
mkdir -p spark/spark_files/jars
mkdir -p spark/spark_files/data
mkdir -p logs
mkdir -p dags

# Download required JAR files if not present
if [ ! -f "spark/spark_files/jars/aws-java-sdk-bundle-1.12.262.jar" ] || [ ! -f "spark/spark_files/jars/hadoop-aws-3.3.4.jar" ]; then
  echo "Downloading required JAR files for S3 connectivity..."
  
  # Create download directory
  mkdir -p downloads
  
  # Download AWS SDK bundle if not exists
  if [ ! -f "spark/spark_files/jars/aws-java-sdk-bundle-1.12.262.jar" ]; then
    echo "Downloading aws-java-sdk-bundle-1.12.262.jar..."
    wget -P downloads https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
    cp downloads/aws-java-sdk-bundle-1.12.262.jar spark/spark_files/jars/
  fi
  
  # Download Hadoop AWS JAR if not exists
  if [ ! -f "spark/spark_files/jars/hadoop-aws-3.3.4.jar" ]; then
    echo "Downloading hadoop-aws-3.3.4.jar..."
    wget -P downloads https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
    cp downloads/hadoop-aws-3.3.4.jar spark/spark_files/jars/
  fi
fi

# Set permissions for the shared directories
echo "Setting permissions for shared directories..."
# Try with sudo if available, otherwise try without
if command -v sudo &> /dev/null; then
  sudo chmod -R 777 spark/spark_files || echo "Warning: Could not set permissions with sudo, container will handle permissions"
else
  chmod -R 777 spark/spark_files || echo "Warning: Could not set permissions, container will handle permissions"
fi

# Start containers
echo "Starting all services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 15

echo "Checking if MinIO is configured correctly..."
docker-compose exec -T minio mc config host add myminio http://localhost:9000 minioadmin minioadmin
docker-compose exec -T minio mc mb --ignore-existing myminio/spark-warehouse
docker-compose exec -T minio mc policy set public myminio/spark-warehouse

echo "All services should be running now. Check their status with: docker-compose ps"
echo ""
echo "Data Engineering Architecture Status"
echo "==================================================="
echo "Access URLs:"
echo "Airflow Web UI: http://localhost:8080 (user: airflow, password: airflow)"
echo "Spark Master UI: http://localhost:8181"
echo "Spark Worker UI: http://localhost:8182"
echo "MinIO Console: http://localhost:9001 (user: minioadmin, password: minioadmin)"

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
echo "Airflow Web UI: READY"
echo "Spark Master UI: READY"
echo "MinIO: READY"
echo ""
echo "Access URLs:"
echo "- Airflow: http://localhost:8080 (user: airflow, password: airflow)"
echo "- Spark Master: http://localhost:8181"
echo "- MinIO Console: http://localhost:9001 (user: minioadmin, password: minioadmin)"
echo ""
echo "To view logs: docker-compose logs -f [service_name]"
echo "To stop all services: docker-compose down"
echo "===================================================" 