# Architecture Description

This document provides details about the data engineering architecture.

## Components

1. **Apache Airflow**
   - Role: Workflow orchestration, scheduling, and monitoring
   - Implementation: Docker container with custom image including PySpark
   - Components:
     - Webserver: UI interface
     - Scheduler: Manages DAG execution
     - Worker: Executes tasks

2. **Apache Spark (Single Node)**
   - Role: Data processing engine
   - Implementation: Single-node Spark setup in Docker
   - Components:
     - Master: Manages and schedules tasks
     - Worker: Executes tasks (co-located with master in single-node setup)

3. **MinIO**
   - Role: S3-compatible object storage
   - Implementation: Docker container
   - Features:
     - S3 API compatibility
     - Web console for browser-based management
     - Bucket-based organization

4. **Data Flow**
   - Source data is loaded from CSV files or generated
   - PySpark jobs process the data in Spark
   - Processed data is stored in MinIO
   - Analytics are performed on the processed data
   - Results are stored back in MinIO

## Network Configuration

- All services communicate within a Docker network
- Ports exposed to host:
  - Airflow Web UI: 8080
  - Spark Master UI: 8181
  - MinIO API: 9000
  - MinIO Console: 9001

## Data Storage

- Raw Data: Stored temporarily in shared volume
- Processed Data: Stored in MinIO in the following structure:
  - `spark-warehouse/breweries/` - Brewery data
  - `spark-warehouse/beers/` - Beer data
  - `spark-warehouse/reviews/` - Review data (partitioned by year/month)
  - `spark-warehouse/analytics/` - Analytics results

## Workflow

1. **Data Ingestion**:
   - Raw data is generated or loaded
   - Data is staged for processing

2. **Data Processing**:
   - PySpark jobs transform the data
   - Data quality checks are performed
   - Transformed data is saved to MinIO

3. **Analytics**:
   - Analytics are performed on the processed data
   - Results are stored in MinIO
   - Visualizations can be created (not included in this architecture) 