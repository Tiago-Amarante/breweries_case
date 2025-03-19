FROM apache/airflow:2.7.3-python3.9

USER root

# Install OpenJDK and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    wget \
    git \
    build-essential \
    libsasl2-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Create spark_files directories with proper permissions
RUN mkdir -p /opt/spark_files/jars && \
    mkdir -p /opt/spark_files/data && \
    chmod -R 777 /opt/spark_files

USER airflow

# First remove any potential conflicting providers
RUN pip uninstall -y apache-airflow-providers-openlineage

# Install PySpark and other Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-openlineage>=1.8.0 \
    boto3 \
    s3fs \
    pandas \
    pyarrow \
    minio \
    duckdb==0.9.2

# Set environment variables
ENV SPARK_HOME=/home/airflow/.local/lib/python3.9/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Create connection file for Spark
COPY spark-defaults.conf $SPARK_HOME/conf/

# Create a custom entrypoint script
COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER airflow

WORKDIR $AIRFLOW_HOME

ENTRYPOINT ["/entrypoint.sh"] 