FROM bitnami/spark:3.4.1

USER root

# Install Python and necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install S3 dependencies for connecting to MinIO
RUN pip3 install --no-cache-dir \
    boto3 \
    s3fs \
    pyspark==3.4.1 \
    pandas \
    pyarrow

# Add Hadoop AWS jars
RUN mkdir -p /opt/bitnami/spark/jars
COPY jars/* /opt/bitnami/spark/jars/

# Copy the Spark configuration file
COPY spark-defaults.conf /opt/bitnami/spark/conf/

# Set environment variables to ensure Python version compatibility with Airflow
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Create shared directories for data exchange
RUN mkdir -p /opt/spark_files && \
    chmod -R 777 /opt/spark_files

USER 1001 