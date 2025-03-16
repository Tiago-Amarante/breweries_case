"""
Advanced Airflow DAG demonstrating a complete ETL pipeline using PySpark with MinIO integration.

This DAG showcases:
1. Extracting data from a CSV file
2. Transforming the data using PySpark
3. Loading the processed data into MinIO (S3-compatible storage)
4. Creating aggregated views for analytics
"""

from datetime import datetime, timedelta
import os
import sys
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, avg, count, desc, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
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
            .appName("Brewery ETL Pipeline")
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

def generate_sample_data(**kwargs):
    """Generate a sample dataset and save as CSV for ingestion."""
    # Define the data directory
    data_dir = "/opt/spark_files/data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Create sample brewery data
    breweries = [
        {"brewery_id": 1, "name": "Hoppy Brewers", "city": "Portland", "state": "OR", "country": "USA", "year_established": 2010, "rating": 4.5},
        {"brewery_id": 2, "name": "Mountain View Brewing", "city": "Denver", "state": "CO", "country": "USA", "year_established": 2015, "rating": 4.2},
        {"brewery_id": 3, "name": "Urban Fermentation", "city": "Chicago", "state": "IL", "country": "USA", "year_established": 2012, "rating": 4.7},
        {"brewery_id": 4, "name": "Coastal Ales", "city": "San Diego", "state": "CA", "country": "USA", "year_established": 2008, "rating": 4.3},
        {"brewery_id": 5, "name": "Northern Lights Brewing", "city": "Seattle", "state": "WA", "country": "USA", "year_established": 2011, "rating": 4.6},
    ]
    
    # Create sample beer data
    beers = [
        {"beer_id": 101, "brewery_id": 1, "name": "Hoppy IPA", "style": "IPA", "abv": 6.8, "ibu": 65, "rating": 4.4},
        {"beer_id": 102, "brewery_id": 1, "name": "Summer Wheat", "style": "Wheat Beer", "abv": 5.2, "ibu": 25, "rating": 4.1},
        {"beer_id": 103, "brewery_id": 2, "name": "Alpine Lager", "style": "Lager", "abv": 4.8, "ibu": 30, "rating": 4.0},
        {"beer_id": 104, "brewery_id": 2, "name": "Rocky Stout", "style": "Stout", "abv": 7.2, "ibu": 40, "rating": 4.5},
        {"beer_id": 105, "brewery_id": 3, "name": "City Porter", "style": "Porter", "abv": 5.9, "ibu": 35, "rating": 4.2},
        {"beer_id": 106, "brewery_id": 3, "name": "Lake Effect Pale Ale", "style": "Pale Ale", "abv": 5.4, "ibu": 45, "rating": 4.3},
        {"beer_id": 107, "brewery_id": 4, "name": "Pacific Pilsner", "style": "Pilsner", "abv": 4.5, "ibu": 28, "rating": 3.9},
        {"beer_id": 108, "brewery_id": 4, "name": "Sunset Amber", "style": "Amber Ale", "abv": 5.6, "ibu": 32, "rating": 4.0},
        {"beer_id": 109, "brewery_id": 5, "name": "Aurora Saison", "style": "Saison", "abv": 6.2, "ibu": 30, "rating": 4.6},
        {"beer_id": 110, "brewery_id": 5, "name": "Evergreen IPA", "style": "IPA", "abv": 7.1, "ibu": 70, "rating": 4.7},
    ]
    
    # Create sample reviews data
    import datetime
    import random
    
    reviews = []
    for i in range(1, 101):
        beer_id = random.choice([101, 102, 103, 104, 105, 106, 107, 108, 109, 110])
        user_id = random.randint(1, 50)
        rating = round(random.uniform(2.5, 5.0), 1)
        review_date = datetime.datetime(2023, random.randint(1, 12), random.randint(1, 28))
        
        reviews.append({
            "review_id": i,
            "beer_id": beer_id,
            "user_id": user_id,
            "rating": rating,
            "review_date": review_date.strftime("%Y-%m-%d %H:%M:%S"),
            "review_text": f"Sample review {i} for beer {beer_id}"
        })
    
    # Save the data as CSV files
    pd.DataFrame(breweries).to_csv(f"{data_dir}/breweries.csv", index=False)
    pd.DataFrame(beers).to_csv(f"{data_dir}/beers.csv", index=False)
    pd.DataFrame(reviews).to_csv(f"{data_dir}/reviews.csv", index=False)
    
    print(f"Generated sample data in {data_dir}")
    return data_dir

def extract_transform_breweries(**kwargs):
    """Extract brewery data, perform transformations, and load to MinIO."""
    spark = create_spark_session()
    data_dir = kwargs['ti'].xcom_pull(task_ids='generate_sample_data')
    
    # Define schema for breweries
    brewery_schema = StructType([
        StructField("brewery_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("year_established", IntegerType(), True),
        StructField("rating", FloatType(), True)
    ])
    
    # Read brewery data
    breweries_df = spark.read.csv(
        f"file://{data_dir}/breweries.csv",
        header=True,
        schema=brewery_schema
    )
    
    # Transformations - cleanse and enrich data
    transformed_df = (breweries_df
        .withColumn("name", col("name").cast(StringType()))
        .withColumn("age_years", lit(2023) - col("year_established"))
        .withColumn("brewery_id", col("brewery_id").cast(IntegerType()))
    )
    
    # Show transformed data
    print("Transformed Breweries Data:")
    transformed_df.show()
    
    # Write to MinIO
    transformed_df.write \
        .mode("overwrite") \
        .parquet("s3a://spark-warehouse/breweries/")
    
    print("Brewery data written to MinIO at s3a://spark-warehouse/breweries/")
    
    spark.stop()

def extract_transform_beers(**kwargs):
    """Extract beer data, perform transformations, and load to MinIO."""
    spark = create_spark_session()
    data_dir = kwargs['ti'].xcom_pull(task_ids='generate_sample_data')
    
    # Define schema for beers
    beer_schema = StructType([
        StructField("beer_id", IntegerType(), True),
        StructField("brewery_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("style", StringType(), True),
        StructField("abv", FloatType(), True),
        StructField("ibu", IntegerType(), True),
        StructField("rating", FloatType(), True)
    ])
    
    # Read beer data
    beers_df = spark.read.csv(
        f"file://{data_dir}/beers.csv",
        header=True,
        schema=beer_schema
    )
    
    # Transformations
    transformed_df = (beers_df
        .withColumn("name", col("name").cast(StringType()))
        .withColumn("beer_id", col("beer_id").cast(IntegerType()))
        .withColumn("brewery_id", col("brewery_id").cast(IntegerType()))
        # Add a simple category based on ABV
        .withColumn("strength_category", 
                   (col("abv") < 5.0).cast("int") * 1 + 
                   ((col("abv") >= 5.0) & (col("abv") < 7.0)).cast("int") * 2 +
                   (col("abv") >= 7.0).cast("int") * 3)
    )
    
    # Show transformed data
    print("Transformed Beers Data:")
    transformed_df.show()
    
    # Write to MinIO
    transformed_df.write \
        .mode("overwrite") \
        .parquet("s3a://spark-warehouse/beers/")
    
    print("Beer data written to MinIO at s3a://spark-warehouse/beers/")
    
    spark.stop()

def extract_transform_reviews(**kwargs):
    """Extract review data, perform transformations, and load to MinIO."""
    spark = create_spark_session()
    data_dir = kwargs['ti'].xcom_pull(task_ids='generate_sample_data')
    
    # Define schema for reviews
    review_schema = StructType([
        StructField("review_id", IntegerType(), True),
        StructField("beer_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("review_date", TimestampType(), True),
        StructField("review_text", StringType(), True)
    ])
    
    # Read review data
    reviews_df = spark.read.csv(
        f"file://{data_dir}/reviews.csv",
        header=True,
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )
    
    # Transformations
    transformed_df = (reviews_df
        .withColumn("review_id", col("review_id").cast(IntegerType()))
        .withColumn("beer_id", col("beer_id").cast(IntegerType()))
        .withColumn("user_id", col("user_id").cast(IntegerType()))
        .withColumn("rating", col("rating").cast(FloatType()))
        .withColumn("review_date", col("review_date").cast(TimestampType()))
        .withColumn("year", year(col("review_date")))
        .withColumn("month", month(col("review_date")))
        .withColumn("day", dayofmonth(col("review_date")))
    )
    
    # Show transformed data
    print("Transformed Reviews Data:")
    transformed_df.show()
    
    # Write to MinIO with partitioning
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("s3a://spark-warehouse/reviews/")
    
    print("Review data written to MinIO at s3a://spark-warehouse/reviews/")
    
    spark.stop()

def create_analytics_views(**kwargs):
    """Create analytics views from the processed data."""
    spark = create_spark_session()
    
    # Read the transformed data from MinIO
    breweries_df = spark.read.parquet("s3a://spark-warehouse/breweries/")
    beers_df = spark.read.parquet("s3a://spark-warehouse/beers/")
    reviews_df = spark.read.parquet("s3a://spark-warehouse/reviews/")
    
    # Register as temp views for SQL
    breweries_df.createOrReplaceTempView("breweries")
    beers_df.createOrReplaceTempView("beers")
    reviews_df.createOrReplaceTempView("reviews")
    
    # 1. Top-rated beers
    top_beers = spark.sql("""
        SELECT b.beer_id, b.name, b.style, b.abv, AVG(r.rating) as avg_rating, COUNT(r.review_id) as num_reviews
        FROM beers b
        JOIN reviews r ON b.beer_id = r.beer_id
        GROUP BY b.beer_id, b.name, b.style, b.abv
        HAVING COUNT(r.review_id) >= 5
        ORDER BY avg_rating DESC
        LIMIT 10
    """)
    
    print("Top Rated Beers:")
    top_beers.show()
    
    # Save to MinIO
    top_beers.write.mode("overwrite").parquet("s3a://spark-warehouse/analytics/top_beers/")
    
    # 2. Brewery performance
    brewery_performance = spark.sql("""
        SELECT 
            br.brewery_id, 
            br.name as brewery_name, 
            br.city, 
            br.state,
            COUNT(DISTINCT b.beer_id) as num_beers,
            AVG(b.rating) as avg_beer_rating,
            AVG(r.rating) as avg_review_rating,
            COUNT(r.review_id) as total_reviews
        FROM breweries br
        JOIN beers b ON br.brewery_id = b.brewery_id
        LEFT JOIN reviews r ON b.beer_id = r.beer_id
        GROUP BY br.brewery_id, br.name, br.city, br.state
        ORDER BY avg_review_rating DESC
    """)
    
    print("Brewery Performance:")
    brewery_performance.show()
    
    # Save to MinIO
    brewery_performance.write.mode("overwrite").parquet("s3a://spark-warehouse/analytics/brewery_performance/")
    
    # 3. Beer style popularity
    style_popularity = spark.sql("""
        SELECT 
            b.style,
            COUNT(DISTINCT b.beer_id) as num_beers,
            AVG(b.abv) as avg_abv,
            AVG(b.ibu) as avg_ibu,
            AVG(r.rating) as avg_rating,
            COUNT(r.review_id) as total_reviews
        FROM beers b
        LEFT JOIN reviews r ON b.beer_id = r.beer_id
        GROUP BY b.style
        ORDER BY total_reviews DESC
    """)
    
    print("Beer Style Popularity:")
    style_popularity.show()
    
    # Save to MinIO
    style_popularity.write.mode("overwrite").parquet("s3a://spark-warehouse/analytics/style_popularity/")
    
    spark.stop()

# Define the DAG
with DAG(
    'brewery_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for brewery data using PySpark and MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['pyspark', 'etl', 'minio', 'brewery'],
) as dag:
    
    # Task to generate sample data
    generate_data_task = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data,
    )
    
    # Tasks to process each dataset
    process_breweries_task = PythonOperator(
        task_id='process_breweries',
        python_callable=extract_transform_breweries,
        provide_context=True,
    )
    
    process_beers_task = PythonOperator(
        task_id='process_beers',
        python_callable=extract_transform_beers,
        provide_context=True,
    )
    
    process_reviews_task = PythonOperator(
        task_id='process_reviews',
        python_callable=extract_transform_reviews,
        provide_context=True,
    )
    
    # Task to create analytics views
    create_analytics_task = PythonOperator(
        task_id='create_analytics',
        python_callable=create_analytics_views,
        provide_context=True,
    )
    
    # Define task dependencies
    generate_data_task >> [process_breweries_task, process_beers_task, process_reviews_task] >> create_analytics_task 