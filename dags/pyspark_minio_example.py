"""
Example Airflow DAG to demonstrate PySpark with MinIO integration.
This DAG demonstrates:
1. Creating a sample DataFrame
2. Writing the DataFrame to MinIO
3. Reading the DataFrame back from MinIO
4. Performing a simple transformation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, count
import os
import sys
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_spark_session():
    """Create a Spark session configured for MinIO."""
    # Get Python executable path
    python_executable = sys.executable
    print(f"Using Python executable: {python_executable}")
    print(f"Python version: {sys.version}")
    
    # Force same Python executable for driver and workers
    os.environ['PYSPARK_PYTHON'] = python_executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable
    
    # Load additional JARs for S3 connectivity
    spark_jars_dir = "/opt/spark_files/jars"
    if not os.path.exists(spark_jars_dir):
        os.makedirs(spark_jars_dir, exist_ok=True)
    
    # Copy JAR files from spark/jars to a location accessible to Airflow
    os.system("cp /opt/spark/jars/* /opt/spark_files/jars/ 2>/dev/null || true")
    
    # Find all JAR files
    jars = []
    if os.path.exists(spark_jars_dir):
        jars = [os.path.join(spark_jars_dir, jar) for jar in os.listdir(spark_jars_dir) if jar.endswith('.jar')]
    
    jars_list = ",".join(jars)
    print(f"Loading JAR files: {jars_list}")
    
    return (SparkSession.builder
            .appName("PySpark MinIO Example")
            # Use local mode to avoid Python version conflicts
            .master("local[*]")  
            # Add JARs if found
            .config("spark.jars", jars_list)
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
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

def get_breweries():
    url = "https://api.openbrewerydb.org/breweries"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
        breweries = response.json()
        return breweries
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def create_bronze_data():
    """Create sample data and save to MinIO."""
    spark = create_spark_session()
    
    # Create a sample DataFrame
    data = get_breweries()

    schema = StructType([
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
    df = spark.createDataFrame(data, schema=schema)
        
    # Write the DataFrame to MinIO
    df.write.mode("append").parquet("s3a://bronze/breweries")
    
    print("Data written to MinIO at s3a://bronze/breweries")
    spark.stop()


def create_silver_data():
    """Read data from MinIO and perform a transformation."""
    spark = create_spark_session()
    
    # Read DataFrame from MinIO
    df = spark.read.parquet("s3a://bronze/breweries")

    
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

    # Remove duplicates based on name, city, and country
    df_dropped = df_selected.dropDuplicates(["name", "city", "country"])
    # Save as Delta table partitioned by country and state
    df_dropped.write.mode("overwrite").partitionBy("country","city").format("parquet").save("s3a://silver/breweries")
    
    spark.stop()

def create_gold_data():
    """Perform analytics on the data."""
    spark = create_spark_session()
    
    # Read transformed DataFrame from MinIO
    df_silver = spark.read.format("parquet").load("s3a://silver/breweries")
    
    df_aggregated = df_silver.groupBy("country", "state", "city", "brewery_type").agg(count("id").alias("brewery_count"))
    
    # Save as parquet table
    df_aggregated.write.mode("overwrite").format("parquet").save("s3a://gold/breweries_per_location")
    

    spark.stop()

# Define the DAG
with DAG(
    'pyspark_minio_example',
    default_args=default_args,
    description='Example DAG showing PySpark with MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,

) as dag:
    
    create_data_task = PythonOperator(
        task_id='create_bronze_data',
        python_callable=create_bronze_data,
    )
    
    transform_data_task = PythonOperator(
        task_id='create_silver_data',
        python_callable=create_silver_data,
    )
    
    analyze_data_task = PythonOperator(
        task_id='create_gold_data',
        python_callable=create_gold_data,
    )
    
    # Set task dependencies
    create_data_task >> transform_data_task >> analyze_data_task 