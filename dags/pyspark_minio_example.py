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
from pyspark.sql.functions import col, upper
import os
import sys

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

def create_sample_data():
    """Create sample data and save to MinIO."""
    spark = create_spark_session()
    
    # Create a sample DataFrame
    data = [
        {"id": 1, "name": "Brewery A", "location": "City X", "rating": 4.5},
        {"id": 2, "name": "Brewery B", "location": "City Y", "rating": 4.2},
        {"id": 3, "name": "Brewery C", "location": "City Z", "rating": 4.8},
        {"id": 4, "name": "Brewery D", "location": "City X", "rating": 3.9},
        {"id": 5, "name": "Brewery E", "location": "City Y", "rating": 4.1},
    ]
    
    df = spark.createDataFrame(data)
    
    # Show the DataFrame
    print("Original DataFrame:")
    df.show()
    
    # Write the DataFrame to MinIO
    df.write.mode("overwrite").parquet("s3a://spark-warehouse/breweries")
    
    print("Data written to MinIO at s3a://spark-warehouse/breweries")
    spark.stop()

def read_and_transform_data():
    """Read data from MinIO and perform a transformation."""
    spark = create_spark_session()
    
    # Read DataFrame from MinIO
    df = spark.read.parquet("s3a://spark-warehouse/breweries")
    
    print("DataFrame read from MinIO:")
    df.show()
    
    # Perform a simple transformation
    df_transformed = df.withColumn("name", upper(col("name")))
    
    print("Transformed DataFrame:")
    df_transformed.show()
    
    # Write transformed data back to MinIO
    df_transformed.write.mode("overwrite").parquet("s3a://spark-warehouse/breweries_transformed")
    
    print("Transformed data written to MinIO at s3a://spark-warehouse/breweries_transformed")
    spark.stop()

def analyze_data():
    """Perform analytics on the data."""
    spark = create_spark_session()
    
    # Read transformed DataFrame from MinIO
    df = spark.read.parquet("s3a://spark-warehouse/breweries_transformed")
    
    # Group by location and calculate average rating
    df_analytics = df.groupBy("location").agg({"rating": "avg"})
    df_analytics = df_analytics.withColumnRenamed("avg(rating)", "avg_rating")
    
    print("Analytics Result:")
    df_analytics.show()
    
    # Write analytics results to MinIO
    df_analytics.write.mode("overwrite").parquet("s3a://spark-warehouse/breweries_analytics")
    
    print("Analytics data written to MinIO at s3a://spark-warehouse/breweries_analytics")
    spark.stop()

# Define the DAG
with DAG(
    'pyspark_minio_example',
    default_args=default_args,
    description='Example DAG showing PySpark with MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'pyspark', 'minio'],
) as dag:
    
    create_data_task = PythonOperator(
        task_id='create_sample_data',
        python_callable=create_sample_data,
    )
    
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=read_and_transform_data,
    )
    
    analyze_data_task = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_data,
    )
    
    # Set task dependencies
    create_data_task >> transform_data_task >> analyze_data_task 