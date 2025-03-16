# Brewery Data Pipeline

A complete data engineering architecture using Apache Airflow, Apache Spark, and MinIO for processing and analyzing brewery data.

## Architecture Overview

This project implements a data pipeline that:

1. Generates sample brewery data
2. Processes the data using Apache Spark
3. Stores the processed data in MinIO (S3-compatible storage)
4. Creates analytics views for visualization

## Components

- **Apache Airflow**: Orchestration platform for managing workflows
- **Apache Spark**: Distributed computing system for data processing
- **MinIO**: S3-compatible object storage for data storage
- **PostgreSQL**: Database for Airflow metadata
- **Redis**: Message broker for Airflow's Celery executor

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Git
- 8GB+ RAM recommended

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/brewery-data-pipeline.git
   cd brewery-data-pipeline
   ```

2. Run the start script:
   ```bash
   ./start.sh
   ```
   
   This script will:
   - Create necessary directories
   - Download required JAR files for S3 connectivity
   - Start all services with Docker Compose
   - Configure MinIO buckets

3. Access the services:
   - Airflow UI: http://localhost:8080 (login with username: `airflow`, password: `airflow`)
   - MinIO Console: http://localhost:9001 (login with access key: `minioadmin`, secret key: `minioadmin`)
   - Spark Master UI: http://localhost:8181
   - Spark Worker UI: http://localhost:8182

## DAGs (Directed Acyclic Graphs)

This project includes two DAGs:

1. **pyspark_minio_example**: Simple example showcasing PySpark with MinIO integration
   - Creates sample brewery data
   - Writes it to MinIO
   - Reads it back and performs transformations
   - Performs simple analytics

2. **brewery_etl_pipeline**: Complete ETL pipeline for brewery data
   - Generates sample data for breweries, beers, and reviews
   - Processes each dataset with transformations
   - Loads data into MinIO
   - Creates analytics views for reporting

## Architecture Improvements

This architecture stores all data in MinIO (S3-compatible storage) instead of using local storage, providing:

- Better scalability - MinIO can handle much larger datasets
- Improved reliability - Data is stored in a dedicated object storage service
- Better separation of concerns - Compute (Spark) is separated from storage (MinIO)
- Easier access to data from multiple services

## Troubleshooting

If you encounter any issues:

1. Check container logs:
   ```bash
   docker-compose logs [service_name]
   ```

2. Ensure all services are running:
   ```bash
   docker-compose ps
   ```

3. Restart all services:
   ```bash
   docker-compose down
   ./start.sh
   ```

## Stopping the Services

To stop all services:
```bash
docker-compose down
```

To remove all data and start fresh:
```bash
docker-compose down -v
rm -rf logs/* spark_files/data/*
./start.sh
``` 