# Brewery Data ETL Pipeline

A modern data engineering platform for brewery data processing and analytics, implementing a Bronze-Silver-Gold data architecture with comprehensive testing.

## Architecture Overview

This project implements a scalable data engineering solution with the following features:

- **Mediated Data Lake Architecture**: Bronze (raw) → Silver (transformed) → Gold (aggregated) layers
- **Containerized Execution**: All components run in Docker containers for easy deployment
- **API Data Ingestion**: Extracts brewery data from the Open Brewery DB API
- **Distributed Processing**: Uses Apache Spark for scalable data transformations
- **Object Storage**: MinIO provides S3-compatible storage for all data layers
- **SQL Analytics**: DuckDB enables direct SQL queries on stored data
- **Workflow Orchestration**: Apache Airflow manages the ETL pipeline execution
- **Comprehensive Testing**: Unit and integration tests for all pipeline components

## System Architecture Diagram

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Open       │    │  Apache     │    │  Apache     │    │  MinIO      │
│  Brewery DB │───▶│  Airflow    │───▶│  Spark      │───▶│  Storage    │
│  API        │    │  Scheduler  │    │  Processing │    │  (S3)       │
└─────────────┘    └─────────────┘    └─────────────┘    └──────┬──────┘
                                                                 │
                                                                 ▼
                                                         ┌─────────────┐
                                                         │  DuckDB     │
                                                         │  SQL Query  │
                                                         └─────────────┘
```

## Data Flow

1. **Bronze Layer**: Raw data extracted from Open Brewery DB API
2. **Silver Layer**: Cleaned, transformed data with standardized formatting
3. **Gold Layer**: Aggregated analytical data ready for business intelligence

## Components

- **Apache Airflow**: Workflow orchestration (DAGs, tasks, scheduling)
- **Apache Spark**: Distributed data processing engine
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Metadata store for Airflow
- **Redis**: Message broker for Airflow's Celery executor
- **DuckDB**: In-process analytical SQL engine for querying MinIO data

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Git
- 8GB+ RAM recommended
- Linux or macOS environment

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd brewery-data-pipeline
   ```

2. Run the start script:
   ```bash
   ./start.sh
   ```

   This script will:
   - Create necessary directories
   - Download required JAR files for Spark-MinIO connectivity
   - Start all services using Docker Compose
   - Configure MinIO buckets (bronze, silver, gold)
   - Ensure proper permissions for data directories

3. Verify that all services are running:
   ```bash
   docker-compose ps
   ```

### Access Points

Once the system is running, you can access the following interfaces:

- **Airflow UI**: [http://localhost:8080](http://localhost:8080)
  - Username: `airflow`
  - Password: `airflow`

- **MinIO Console**: [http://localhost:9001](http://localhost:9001)
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin`

- **Spark Master UI**: [http://localhost:8181](http://localhost:8181)

- **Spark Worker UI**: [http://localhost:8182](http://localhost:8182)

- **DuckDB JupyterLab**: [http://localhost:8888](http://localhost:8888)

## ETL Pipeline

### Bronze-Silver-Gold Architecture

The project implements a modern data lake architecture:

1. **Bronze Layer**: 
   - Raw data ingestion from the Open Brewery DB API
   - Stored in S3a://bronze/breweries
   - Preserves original format and data
   - Append-only data collection

2. **Silver Layer**:
   - Data cleansing and transformation
   - Schema standardization with renamed columns
   - Null value handling (fills missing values with "Unknown")
   - Deduplication based on name, city, and country
   - Partitioned by country and city
   - Stored in S3a://silver/breweries

3. **Gold Layer**:
   - Business-level aggregations
   - Brewery counts by location and type
   - Ready for analytics and reporting
   - Stored in S3a://gold/breweries_per_location

### Brewery DAG

The main pipeline is implemented in the `brewery_etl_pipeline` DAG:

1. `create_bronze_data`: Extracts brewery data from API and stores it
2. `create_silver_data`: Cleans and transforms the bronze data
3. `create_gold_data`: Aggregates the silver data for analytics

## Testing

The project includes comprehensive testing capabilities:

### Unit Tests

Tests for individual components of the pipeline:

- Bronze layer tests (API extraction, data validation)
- Silver layer tests (transformation, cleaning)
- Gold layer tests (aggregation, analytics)

### Integration Tests

End-to-end tests that verify the entire pipeline's functionality:

- Full ETL pipeline flow
- Data quality verification
- Error handling

## Using DuckDB for Analytics

The project includes DuckDB for SQL analytics directly on the MinIO data:

1. Access JupyterLab at [http://localhost:8888](http://localhost:8888)
2. Use example notebooks or create a new Python notebook
3. Connect to MinIO and run SQL queries:

OBS. I hardly try to query minio buckets using DuckDb, but I'm missing something. I can't do it. My theory is something in bucket permission

```python
import duckdb

# Create a connection
conn = duckdb.connect(':memory:')

# Load extensions for S3 access
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("INSTALL parquet; LOAD parquet;")

# Configure S3 connection
conn.execute("""
SET s3_endpoint='http://minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_region='us-east-1';
SET s3_url_style='path';
""")

# Query bronze layer
bronze_df = conn.execute("SELECT * FROM 's3://bronze/breweries' LIMIT 10").fetchdf()
print("Bronze data:")
print(bronze_df)

# Query silver layer
silver_df = conn.execute("SELECT * FROM 's3://silver/breweries' LIMIT 10").fetchdf()
print("Silver data:")
print(silver_df)

# Query gold layer
gold_df = conn.execute("SELECT * FROM 's3://gold/breweries_per_location' LIMIT 10").fetchdf()
print("Gold data:")
print(gold_df)

# Run analytics queries
analytics = conn.execute("""
SELECT country, SUM(brewery_count) as total_breweries 
FROM 's3://gold/breweries_per_location' 
GROUP BY country 
ORDER BY total_breweries DESC
""").fetchdf()
print("Analytics results:")
print(analytics)
```

## Project Structure

```
brewery-data-pipeline/
├── airflow/                  # Airflow configuration files
├── dags/                     # Airflow DAG definitions
│   └── pyspark_b_s_g_etls.py # Main ETL pipeline
├── duckdb/                   # DuckDB configuration and notebooks
│   ├── Dockerfile            # DuckDB container setup
│   ├── notebooks/            # Jupyter notebooks for analytics
│   └── test_duckdb.sh        # DuckDB connection test script
├── logs/                     # Log files directory
├── plugins/                  # Airflow plugins directory
├── spark_files/              # Spark data and JAR files
│   └── jars/                 # JARs for S3 connectivity
├── docker-compose.yml        # Service definitions
├── README.md                 # This documentation
├── start.sh                  # Script to start all services
└── stop.sh                   # Script to stop all services
```

## Maintenance and Troubleshooting

### Checking Service Status

```bash
docker-compose ps
```

### Viewing Logs

```bash
docker-compose logs [service_name]
```

### Restarting Services

```bash
docker-compose restart [service_name]
```

### Completely Resetting the Environment

```bash
docker-compose down -v
rm -rf logs/* spark_files/data/*
./start.sh
```

### Common Issues

1. **MinIO Connection Failures**: 
   - Check MinIO is running: `docker-compose ps minio`
   - Verify network connectivity between containers
   - Confirm access credentials are correct

2. **Spark Execution Errors**:
   - Check Spark logs: `docker-compose logs spark`
   - Ensure JAR files for S3 connectivity are present
   - Verify enough memory is allocated to Spark

3. **Airflow DAG Issues**:
   - Check the Airflow UI for error messages
   - View logs in Airflow UI or `logs/` directory
   - Verify dependencies between tasks

## Stopping the Environment

To stop all services:

```bash
./stop.sh
```

This preserves all data in volumes. To delete all data, use:

```bash
docker-compose down -v
```
