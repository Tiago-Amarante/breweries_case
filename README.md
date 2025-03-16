# Data Engineering Architecture: Spark on Airflow with MinIO

This project provides a complete data engineering architecture that integrates:
- Apache Airflow for workflow orchestration
- Apache Spark (single-node) for data processing
- MinIO for S3-compatible object storage
- PySpark for Python-based data processing

## Architecture Overview

![Architecture Diagram](docs/architecture.png)

The architecture consists of the following components:

1. **Apache Airflow**: Handles workflow orchestration, scheduling, and monitoring of data pipelines
2. **Apache Spark (Single Node)**: Processes data using the Spark engine
3. **MinIO**: Provides S3-compatible object storage for storing processed data
4. **PySpark**: Python API for Apache Spark used in Airflow DAGs

## Directory Structure

```
.
├── airflow/                # Airflow Docker configuration
│   ├── Dockerfile          # Custom Airflow image with PySpark
│   └── spark-defaults.conf # Spark configuration for Airflow
├── dags/                   # Airflow DAG files
│   ├── pyspark_minio_example.py     # Simple example DAG
│   └── pyspark_etl_pipeline.py      # Complete ETL pipeline example
├── logs/                   # Airflow logs (mounted volume)
├── plugins/                # Airflow plugins (mounted volume)
├── spark/                  # Spark Docker configuration
│   ├── Dockerfile          # Spark Docker image
│   ├── spark-defaults.conf # Spark configuration
│   ├── download_jars.sh    # Script to download required JARs
│   └── jars/               # Directory for Hadoop/AWS JARs
├── spark_files/            # Shared files between Airflow and Spark
│   └── data/               # Sample data directory (created at runtime)
├── spark_data/             # Local storage for Spark data processing
└── docker-compose.yml      # Docker Compose configuration file
```

## Prerequisites

- Docker
- Docker Compose
- Git
- 8GB+ RAM recommended for running all services
- curl (for the start script to check service health)

## Detailed Setup Instructions

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### Step 2: Run the Start Script

The most straightforward way to set up the environment is to use the provided start script:

```bash
chmod +x start.sh
./start.sh
```

This script will:
1. Create all necessary directories
2. Download required JAR files for S3 connectivity
3. Start all services using Docker Compose
4. Monitor service health and display access information
5. Create required directories in Airflow containers for local Spark processing

### Step 3: Manual Setup (Alternative)

If you prefer a manual setup:

1. Create necessary directories:
   ```bash
   mkdir -p logs plugins spark_files/data spark_files/jars spark_data
   ```

2. Download required JAR files:
   ```bash
   cd spark
   chmod +x download_jars.sh
   ./download_jars.sh
   cd ..
   ```

3. Build and start the services:
   ```bash
   docker-compose up -d
   ```

4. Create the spark data directory in Airflow containers:
   ```bash
   docker exec <airflow-webserver-container-name> mkdir -p /opt/airflow/spark_data
   docker exec <airflow-worker-container-name> mkdir -p /opt/airflow/spark_data
   docker exec <airflow-scheduler-container-name> mkdir -p /opt/airflow/spark_data
   ```

### Step 4: Access the Services

Once the services are up and running:

- **Airflow UI**: http://localhost:8080 (user: airflow, password: airflow)
- **Spark Master UI**: http://localhost:8181
- **MinIO Console**: http://localhost:9001 (user: minioadmin, password: minioadmin)

## Python Version Compatibility

This architecture has been configured to handle potential Python version differences between containers. The DAGs use local Spark execution mode and ensure consistent Python environments by:

1. Using the same Python executable for both driver and worker processes
2. Running Spark in local mode within the Airflow worker containers
3. Using local filesystem storage instead of MinIO for data persistence

This approach ensures compatibility even when Airflow and Spark containers use different Python versions.

## Example DAGs

### 1. Simple PySpark Example

This DAG demonstrates:
- Creating a sample DataFrame in PySpark
- Writing data to local storage
- Reading data from local storage
- Performing simple transformations

To run this example:
1. Go to the Airflow UI (http://localhost:8080)
2. Navigate to DAGs
3. Enable and trigger the `pyspark_minio_example` DAG

### 2. Complete ETL Pipeline

This DAG demonstrates a complete ETL pipeline:
- Extracting data from CSV files
- Transforming the data using PySpark
- Loading the processed data into local storage
- Creating analytics views for reporting

To run this example:
1. Go to the Airflow UI (http://localhost:8080)
2. Navigate to DAGs
3. Enable and trigger the `brewery_etl_pipeline` DAG

## Monitoring and Troubleshooting

### Checking Logs

You can view the logs for specific services using Docker Compose:

```bash
docker-compose logs -f airflow-webserver    # Airflow Webserver logs
docker-compose logs -f airflow-scheduler    # Airflow Scheduler logs
docker-compose logs -f airflow-worker       # Airflow Worker logs
docker-compose logs -f spark                # Spark Master logs
docker-compose logs -f spark-worker         # Spark Worker logs
docker-compose logs -f minio                # MinIO logs
```

Airflow task logs can be viewed in the Airflow UI or by checking:

```bash
ls -la logs/dag_id/task_id/year-month-day
```

### Common Issues and Solutions

#### 1. Python Version Mismatch

**Issue**: Error message about different Python versions between driver and worker.
**Solution**: The DAGs are now configured to use local Spark execution mode and the same Python executable, which should avoid this issue.

#### 2. Memory Issues

**Issue**: Docker containers getting killed or out-of-memory errors.
**Solution**: Increase Docker memory limits in Docker Desktop settings or Docker daemon configuration.

#### 3. Connectivity Between Services

**Issue**: Services cannot connect to each other.
**Solution**: Check Docker network configuration and ensure all services are in the same network.

#### 4. S3A/MinIO Connectivity Issues

**Issue**: Cannot connect to MinIO or S3A filesystem errors.
**Solution**: For issues with S3A connectivity, check if the required JAR files are properly downloaded in the `spark/jars/` directory.

## Stopping the Services

To stop all services:

```bash
docker-compose down
```

To stop and remove volumes (this will delete all data):

```bash
docker-compose down -v
```

## Extending the Architecture

To extend this architecture:

1. **Add new DAGs**: Create new Python files in the `dags/` directory
2. **Install additional Python packages**: Modify the `airflow/Dockerfile` or `spark/Dockerfile`
3. **Configure Spark settings**: Modify the `spark-defaults.conf` files
4. **Add new data sources**: Update the extraction steps in your DAGs

## License

This project is licensed under the MIT License - see the LICENSE file for details. 