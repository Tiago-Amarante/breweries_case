#!/bin/bash
set -e

# Ensure JAR files are copied to the shared volume on container start
if [ "$(id -u)" = "0" ]; then
  # If running as root, set proper permissions
  mkdir -p /opt/spark_files/jars
  cp /opt/bitnami/spark/jars/aws-java-sdk-bundle-*.jar /opt/spark_files/jars/ 2>/dev/null || true
  cp /opt/bitnami/spark/jars/hadoop-aws-*.jar /opt/spark_files/jars/ 2>/dev/null || true
  chmod -R 777 /opt/spark_files
else
  # If running as non-root, use original entry point behavior
  echo "Running as non-root user, skipping permission setup"
fi

# Execute the original entry point
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@" 