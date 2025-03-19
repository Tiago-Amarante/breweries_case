"""
Airflow DAG for Brewery Data ETL Pipeline.
This DAG implements a Bronze-Silver-Gold data architecture:
1. Bronze: Raw data from the Open Brewery DB API stored in MinIO
2. Silver: Cleaned and transformed data partitioned by country and city
3. Gold: Aggregated analytics data for reporting
"""

from datetime import datetime, timedelta
import os
import sys
import logging
import requests
from typing import List, Dict, Any, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, count

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration 
# In a production environment, these would be stored as Airflow Variables
CONFIG = {
    'api_url': 'https://api.openbrewerydb.org/breweries',
    'minio_endpoint': 'http://minio:9000',
    'minio_access_key': 'minioadmin',
    'minio_secret_key': 'minioadmin',
    'bronze_bucket': 's3a://bronze/breweries',
    'silver_bucket': 's3a://silver/breweries',
    'gold_bucket': 's3a://gold/breweries_per_location',
    'spark_jars_dir': '/opt/spark_files/jars',
}

# Schema definitions
BREWERY_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True)
])

def create_spark_session(app_name: str = "PySpark MinIO ETL") -> SparkSession:
    """
    Create a Spark session configured for MinIO.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession object
        
    Raises:
        AirflowException: If unable to create the Spark session
    """
    try:
        # Get Python executable path
        python_executable = sys.executable
        logging.info(f"Using Python executable: {python_executable}")
        logging.info(f"Python version: {sys.version}")
        
        # Force same Python executable for driver and workers
        os.environ['PYSPARK_PYTHON'] = python_executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable
        
        # Load additional JARs for S3 connectivity
        spark_jars_dir = CONFIG['spark_jars_dir']
        if not os.path.exists(spark_jars_dir):
            os.makedirs(spark_jars_dir, exist_ok=True)
        
        # Copy JAR files from spark/jars to a location accessible to Airflow
        os.system("cp /opt/spark/jars/* /opt/spark_files/jars/ 2>/dev/null || true")
        
        # Find all JAR files
        jars = []
        if os.path.exists(spark_jars_dir):
            jars = [os.path.join(spark_jars_dir, jar) for jar in os.listdir(spark_jars_dir) if jar.endswith('.jar')]
        
        jars_list = ",".join(jars)
        logging.info(f"Loading JAR files: {jars_list}")
        
        return (SparkSession.builder
                .appName(app_name)
                # Use local mode to avoid Python version conflicts
                .master("local[*]")  
                # Add JARs if found
                .config("spark.jars", jars_list)
                .config("spark.hadoop.fs.s3a.endpoint", CONFIG['minio_endpoint'])
                .config("spark.hadoop.fs.s3a.access.key", CONFIG['minio_access_key'])
                .config("spark.hadoop.fs.s3a.secret.key", CONFIG['minio_secret_key'])
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                # Add these configurations to fix S3A compatibility issues
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
                # Set modest resource requirements
                .config("spark.executor.memory", "512m")
                .config("spark.driver.memory", "512m")
                # Set Python for PySpark - use the same Python executable
                .config("spark.pyspark.python", python_executable)
                .config("spark.pyspark.driver.python", python_executable)
                .getOrCreate())
    except Exception as e:
        raise AirflowException(f"Failed to create Spark session: {str(e)}")

def get_breweries() -> Optional[List[Dict[str, Any]]]:
    """
    Fetch brewery data from the Open Brewery DB API.
    
    Returns:
        List of brewery dictionaries or None if the API call fails
        
    Raises:
        AirflowException: If the API call fails after retries
    """
    url = CONFIG['api_url']
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
            breweries = response.json()
            if not breweries or not isinstance(breweries, list):
                raise AirflowException(f"Invalid data format from API: {breweries}")
            logging.info(f"Successfully fetched {len(breweries)} breweries from API")
            return breweries
        except requests.exceptions.RequestException as e:
            retry_count += 1
            logging.warning(f"Error fetching data (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                raise AirflowException(f"Failed to fetch brewery data after {max_retries} attempts: {str(e)}")
    
    return None

def validate_brewery_data(breweries: List[Dict[str, Any]]) -> None:
    """
    Validate brewery data structure and content.
    
    Args:
        breweries: List of brewery dictionaries
        
    Raises:
        AirflowException: If data validation fails
    """
    if not breweries:
        raise AirflowException("Empty brewery data received")
    
    required_fields = ["id", "name", "brewery_type", "city", "country"]
    
    for i, brewery in enumerate(breweries):
        missing_fields = [field for field in required_fields if field not in brewery or not brewery[field]]
        if missing_fields:
            logging.warning(f"Brewery at index {i} is missing required fields: {missing_fields}")
            
    logging.info("Brewery data validation complete")

def create_bronze_data() -> None:
    """
    Create bronze layer data by fetching from API and storing raw data in MinIO.
    """
    try:
        spark = create_spark_session("Bronze Brewery ETL")
        
        # Fetch data from API
        breweries = get_breweries()
        
        # Validate data
        validate_brewery_data(breweries)
        
        # Create a DataFrame
        df = spark.createDataFrame(breweries, schema=BREWERY_SCHEMA)
        
        # Write the DataFrame to MinIO
        df.write.mode("append").parquet(CONFIG['bronze_bucket'])
        
        logging.info(f"Bronze data written to MinIO at {CONFIG['bronze_bucket']}")
    except Exception as e:
        logging.error(f"Error in bronze data processing: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()

def create_silver_data() -> None:
    """
    Create silver layer data by cleaning and transforming bronze data.
    """
    try:
        spark = create_spark_session("Silver Brewery ETL")
        
        # Read DataFrame from MinIO
        df = spark.read.parquet(CONFIG['bronze_bucket'])
        
        if df.count() == 0:
            raise AirflowException("No data found in bronze layer")
        
        # Select necessary columns and rename for consistency
        df_selected = df.select(
            col("id"),
            col("name"),
            col("brewery_type"),
            col("address_1").alias("address"),
            col("city"),
            col("state_province").alias("state"),
            col("postal_code"),
            col("country"),
            col("longitude"),
            col("latitude"),
            col("phone"),
            col("website_url")
        )
        
        # Fill missing values for critical fields
        df_cleaned = df_selected.na.fill({
            "country": "Unknown",
            "city": "Unknown",
            "state": "Unknown"
        })
        
        # Remove duplicates based on name, city, and country
        df_dropped = df_cleaned.dropDuplicates(["name", "city", "country"])
        
        # Save as parquet files partitioned by country and city
        df_dropped.write.mode("overwrite").partitionBy("country","city").format("parquet").save(CONFIG['silver_bucket'])
        
        logging.info(f"Silver data written to MinIO at {CONFIG['silver_bucket']}")
    except Exception as e:
        logging.error(f"Error in silver data processing: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()

def create_gold_data() -> None:
    """
    Create gold layer data by aggregating silver data for analytics.
    """
    try:
        spark = create_spark_session("Gold Brewery ETL")
        
        # Read transformed DataFrame from MinIO
        df_silver = spark.read.format("parquet").load(CONFIG['silver_bucket'])
        
        if df_silver.count() == 0:
            raise AirflowException("No data found in silver layer")
        
        # Create brewery count by location and type
        df_location_counts = df_silver.groupBy("country", "state", "city", "brewery_type").agg(
            count("id").alias("brewery_count")
        )
        
        # Save as parquet table
        df_location_counts.write.mode("overwrite").format("parquet").save(CONFIG['gold_bucket'])
        
        logging.info(f"Gold data written to MinIO at {CONFIG['gold_bucket']}")
    except Exception as e:
        logging.error(f"Error in gold data processing: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()

# Define the DAG
with DAG(
    'brewery_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for brewery data using Bronze-Silver-Gold architecture',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['brewery', 'etl', 'pyspark', 'minio'],
) as dag:
    
    create_bronze_task = PythonOperator(
        task_id='create_bronze_data',
        python_callable=create_bronze_data,
    )
    
    create_silver_task = PythonOperator(
        task_id='create_silver_data',
        python_callable=create_silver_data,
    )
    
    create_gold_task = PythonOperator(
        task_id='create_gold_data',
        python_callable=create_gold_data,
    )
    
    # Set task dependencies
    create_bronze_task >> create_silver_task >> create_gold_task 